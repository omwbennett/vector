use std::{
    collections::HashMap,
    convert::Infallible,
    num::NonZeroUsize,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use fluss::{
    client::{AppendWriter, FlussConnection},
    error::Error,
    metadata::{TableInfo, TablePath},
};
use std::sync::Mutex as SyncMutex;
use tokio::{
    sync::Mutex,
    time::{Instant, timeout_at},
};

use super::{
    config::{FlussSinkConfig, build_fluss_config},
    encoding::TableSchema,
};
use crate::sinks::prelude::*;

pub(super) type Delivery = BoxFuture<'static, Result<(), String>>;

#[derive(Debug)]
pub(super) enum SubmitError {
    Rejected(String),
    Retry(String),
    Unknown(String),
}

impl From<Error> for SubmitError {
    fn from(error: Error) -> Self {
        if error.is_retriable()
            || matches!(
                error,
                Error::BufferExhausted { .. } | Error::IoUnexpectedError { .. }
            )
        {
            Self::Retry(error.to_string())
        } else if matches!(
            error,
            Error::IllegalArgument { .. }
                | Error::RowConvertError { .. }
                | Error::ArrowError { .. }
                | Error::UnsupportedOperation { .. }
                | Error::UnsupportedVersion { .. }
                | Error::IoUnsupported { .. }
                | Error::FlussAPIError { .. }
        ) {
            Self::Rejected(error.to_string())
        } else {
            Self::Unknown(error.to_string())
        }
    }
}

pub(super) trait Submit: Send + 'static {
    fn submit<'a>(
        &'a mut self,
        table: &'a str,
        event: &'a Event,
        deadline: Instant,
    ) -> BoxFuture<'a, Result<Delivery, SubmitError>>;
}

pub(super) struct FlussRequest {
    event: Event,
    table: Result<String, String>,
    metadata: RequestMetadata,
    finalizers: EventFinalizers,
    completion: CompletionGuard,
}

// Dropping Vector finalizers without updating them counts as delivery. Cancellation must not.
struct CompletionGuard(EventFinalizers);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.update_status(EventStatus::Errored);
    }
}

impl FlussRequest {
    pub(super) fn new(mut event: Event, table: Result<String, String>) -> Self {
        let size = NonZeroUsize::new(event.size_of().max(1)).expect("positive event size");
        let metadata = RequestMetadataBuilder::from_event(&event).with_request_size(size);
        let finalizers = event.take_finalizers();
        Self {
            event,
            table,
            metadata,
            completion: CompletionGuard(finalizers.clone()),
            finalizers,
        }
    }
}

impl Finalizable for FlussRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for FlussRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }
    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

pub(super) struct FlussResponse {
    status: EventStatus,
    events: GroupedCountByteSize,
}

impl DriverResponse for FlussResponse {
    fn event_status(&self) -> EventStatus {
        self.status
    }
    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events
    }
}

pub(super) struct FlussService<B> {
    backend: Arc<Mutex<B>>,
    request_timeout: Duration,
}

impl<B: Submit> FlussService<B> {
    pub(super) fn new(backend: B, request_timeout: Duration) -> Self {
        Self {
            backend: Arc::new(Mutex::new(backend)),
            request_timeout,
        }
    }
}

impl<B: Submit> Service<FlussRequest> for FlussService<B> {
    type Response = FlussResponse;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The enclosing Tower concurrency limit bounds calls through delivery completion.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: FlussRequest) -> Self::Future {
        let backend = Arc::clone(&self.backend);
        let deadline = Instant::now() + self.request_timeout;
        Box::pin(async move {
            let FlussRequest {
                event,
                table,
                metadata,
                mut completion,
                ..
            } = request;
            let result = async {
                let table = table.map_err(SubmitError::Rejected)?;
                let mut backend = timeout_at(deadline, backend.lock()).await
                    .map_err(|_| SubmitError::Unknown("submission deadline expired waiting for capacity".into()))?;
                let mut backoff = Duration::from_millis(100);
                let delivery = loop {
                    if Instant::now() >= deadline {
                        return Err(SubmitError::Unknown("submission deadline expired".into()));
                    }
                    match backend.submit(&table, &event, deadline).await {
                        Ok(delivery) => break delivery,
                        Err(SubmitError::Retry(error)) => {
                            warn!(message = "Fluss submission failed; retrying before acceptance.", %error, internal_log_rate_limit = true);
                            tokio::time::sleep_until((Instant::now() + backoff).min(deadline)).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                        }
                        Err(error) => return Err(error),
                    }
                };
                drop(backend);
                // Native retries own the accepted write. Never replay it on failure or timeout.
                timeout_at(deadline, delivery).await
                    .map_err(|_| SubmitError::Unknown("delivery deadline expired; write may have committed".into()))?
                    .map_err(SubmitError::Unknown)
            }.await;
            let status = match result {
                Ok(()) => EventStatus::Delivered,
                Err(SubmitError::Rejected(error)) => {
                    error!(message = "Fluss event rejected.", %error, internal_log_rate_limit = true);
                    EventStatus::Rejected
                }
                Err(SubmitError::Unknown(error) | SubmitError::Retry(error)) => {
                    error!(message = "Fluss delivery failed or is unknown.", %error, internal_log_rate_limit = true);
                    vector_lib::counter!(vector_lib::internal_event::CounterName::ComponentErrorsTotal,
                        "error_type" => "request_failed", "stage" => "sending").increment(1);
                    EventStatus::Errored
                }
            };
            // Driver owns the final acknowledgement. Disarm only after a response is available.
            completion.0 = EventFinalizers::default();
            Ok(FlussResponse {
                status,
                events: metadata.into_events_estimated_json_encoded_byte_size(),
            })
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SchemaIdentity {
    table_id: i64,
    schema_id: i32,
}

impl From<&TableInfo> for SchemaIdentity {
    fn from(info: &TableInfo) -> Self {
        Self {
            table_id: info.table_id,
            schema_id: info.schema_id,
        }
    }
}

struct TableWriter {
    writer: Arc<AppendWriter>,
    schema: TableSchema,
    identity: SchemaIdentity,
    checked_at: Option<Instant>,
}

// Kept outside the async submission lock so shutdown can close a blocked native append.
#[derive(Default)]
pub(super) struct ConnectionSlot(SyncMutex<Option<Arc<FlussConnection>>>);

impl ConnectionSlot {
    pub(super) async fn close(&self, timeout: Duration) {
        let connection = self.0.lock().expect("connection lock poisoned").take();
        if let Some(connection) = connection {
            // Native close takes ownership of the sender task; it must finish even if our caller is cancelled.
            let task = tokio::spawn(async move { connection.close(timeout).await });
            match task.await {
                Ok(Ok(())) => {}
                error => error!(message = "Failed to close Fluss connection.", error = ?error),
            }
        }
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        if let Some(connection) = self.0.get_mut().expect("connection lock poisoned").take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                _ = connection.close(Duration::ZERO).await;
            });
        }
    }
}

pub(super) struct NativeSubmitter {
    config: FlussSinkConfig,
    connections: Arc<ConnectionSlot>,
    writers: HashMap<String, TableWriter>,
}

impl NativeSubmitter {
    pub(super) fn new(config: FlussSinkConfig, connections: Arc<ConnectionSlot>) -> Self {
        Self {
            config,
            connections,
            writers: HashMap::new(),
        }
    }

    async fn connection(&self, deadline: Instant) -> Result<Arc<FlussConnection>, SubmitError> {
        if let Some(connection) = self
            .connections
            .0
            .lock()
            .expect("connection lock poisoned")
            .clone()
        {
            return Ok(connection);
        }
        let connection = timeout_at(
            deadline,
            FlussConnection::new(build_fluss_config(&self.config)),
        )
        .await
        .map_err(|_| SubmitError::Retry("connection deadline expired".into()))??;
        let connection = Arc::new(connection);
        *self.connections.0.lock().expect("connection lock poisoned") =
            Some(Arc::clone(&connection));
        Ok(connection)
    }

    async fn reset(&mut self, deadline: Instant) {
        self.writers.clear();
        self.connections
            .close(deadline.saturating_duration_since(Instant::now()))
            .await;
    }

    async fn refresh(&mut self, name: &str, deadline: Instant) -> Result<(), SubmitError> {
        if !self.writers.contains_key(name) && self.writers.len() >= self.config.max_cached_tables {
            self.reset(deadline).await;
        }
        if let Some(cached) = self.writers.get_mut(name) {
            cached.checked_at = None;
        }
        let path = TablePath::new(&self.config.database, name);
        let connection = self.connection(deadline).await?;
        let table = timeout_at(deadline, connection.get_table(&path))
            .await
            .map_err(|_| SubmitError::Retry("metadata deadline expired".into()))??;
        let identity = SchemaIdentity::from(table.get_table_info());
        if let Some(cached) = self.writers.get_mut(name) {
            if cached.identity == identity {
                cached.checked_at = Some(Instant::now());
                return Ok(());
            }
            // The native accumulator does not isolate schema versions. Retire every old batch first.
            self.reset(deadline).await;
        }
        let connection = self.connection(deadline).await?;
        let table = timeout_at(deadline, connection.get_table(&path))
            .await
            .map_err(|_| SubmitError::Retry("metadata deadline expired".into()))??;
        let schema = TableSchema::new(table.get_table_info().get_row_type(), &self.config)
            .map_err(SubmitError::Rejected)?;
        let identity = SchemaIdentity::from(table.get_table_info());
        let writer = Arc::new(table.new_append()?.create_writer()?);
        self.writers.insert(
            name.to_owned(),
            TableWriter {
                writer,
                schema,
                identity,
                checked_at: Some(Instant::now()),
            },
        );
        Ok(())
    }

    async fn try_submit(
        &mut self,
        name: &str,
        event: &Event,
        deadline: Instant,
    ) -> Result<Delivery, SubmitError> {
        let refresh = Duration::from_secs(self.config.schema_refresh_interval_sec);
        if self.writers.get(name).is_none_or(|table| {
            table
                .checked_at
                .is_none_or(|checked| checked.elapsed() >= refresh)
        }) {
            self.refresh(name, deadline).await?;
        }
        let cached = self.writers.get(name).expect("writer loaded");
        let previous_identity = cached.identity;
        let row = match cached.schema.encode(event) {
            Ok(row) => row,
            Err(error) => {
                // One refresh and decode before any append, only if the actual identity changed.
                self.refresh(name, deadline).await?;
                let cached = self.writers.get(name).expect("writer refreshed");
                if cached.identity == previous_identity {
                    return Err(SubmitError::Rejected(error));
                }
                cached.schema.encode(event).map_err(SubmitError::Rejected)?
            }
        };
        let cached = self.writers.get(name).expect("writer loaded");
        let writer = Arc::clone(&cached.writer);
        let identity = cached.identity;
        let connection = self.connection(deadline).await?;
        let path = TablePath::new(&self.config.database, name);
        // One blocking submission at a time, with a finite native buffer wait. Do not wrap this
        // join in an async timeout: dropping it cannot cancel append and must never trigger replay.
        let result = tokio::task::spawn_blocking(move || {
            let metadata = connection.get_metadata();
            let cluster = metadata.get_cluster();
            let current = cluster.get_table(&path)?;
            if SchemaIdentity::from(current) != identity {
                return Ok(None);
            }
            writer.append(&row).map(Some)
        })
        .await
        .map_err(|error| SubmitError::Unknown(format!("append task failed: {error}")))??;
        match result {
            Some(delivery) => Ok(Box::pin(async move {
                delivery.await.map_err(|error| error.to_string())
            })),
            None => {
                self.refresh(name, deadline).await?;
                Err(SubmitError::Retry(
                    "table identity changed before append".into(),
                ))
            }
        }
    }
}

impl Submit for NativeSubmitter {
    fn submit<'a>(
        &'a mut self,
        table: &'a str,
        event: &'a Event,
        deadline: Instant,
    ) -> BoxFuture<'a, Result<Delivery, SubmitError>> {
        Box::pin(self.try_submit(table, event, deadline))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::oneshot;
    use vector_lib::finalization::{BatchNotifier, BatchStatus};

    struct FakeSubmitter {
        attempts: Arc<AtomicUsize>,
        results: VecDeque<Result<Delivery, SubmitError>>,
    }

    impl Submit for FakeSubmitter {
        fn submit<'a>(
            &'a mut self,
            _table: &'a str,
            _event: &'a Event,
            _deadline: Instant,
        ) -> BoxFuture<'a, Result<Delivery, SubmitError>> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            future::ready(self.results.pop_front().expect("unexpected resubmission")).boxed()
        }
    }

    fn pending_delivery() -> (oneshot::Sender<Result<(), String>>, Delivery) {
        let (sender, receiver) = oneshot::channel();
        (sender, async move { receiver.await.unwrap() }.boxed())
    }

    fn requests(
        count: usize,
    ) -> (
        Vec<FlussRequest>,
        vector_lib::finalization::BatchStatusReceiver,
    ) {
        let mut events = (0..count)
            .map(|_| Event::Log(LogEvent::from("test")))
            .collect::<Vec<_>>();
        let receiver = BatchNotifier::apply_to(&mut events);
        (
            events
                .into_iter()
                .map(|event| FlussRequest::new(event, Ok("events".into())))
                .collect(),
            receiver,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn finalizers_wait_for_ack_and_concurrency_is_bounded() {
        for limit in [1, 2] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let (first_tx, first) = pending_delivery();
            let (second_tx, second) = pending_delivery();
            let service =
                ServiceBuilder::new()
                    .concurrency_limit(limit)
                    .service(FlussService::new(
                        FakeSubmitter {
                            attempts: Arc::clone(&attempts),
                            results: VecDeque::from([Ok(first), Ok(second)]),
                        },
                        Duration::from_secs(60),
                    ));
            let (requests, mut status) = requests(2);
            let task = tokio::spawn(stream::iter(requests).into_driver(service).run());
            assert!(
                tokio::time::timeout(Duration::from_millis(1), &mut status)
                    .await
                    .is_err()
            );
            assert_eq!(attempts.load(Ordering::SeqCst), limit);
            first_tx.send(Ok(())).unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(1), &mut status)
                    .await
                    .is_err()
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
            second_tx.send(Ok(())).unwrap();
            assert_eq!(status.await, BatchStatus::Delivered);
            assert_eq!(task.await.unwrap(), Ok(()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn transient_submission_failure_retries_the_retained_event() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let service = FlussService::new(
            FakeSubmitter {
                attempts: Arc::clone(&attempts),
                results: VecDeque::from([
                    Err(SubmitError::Retry("offline".into())),
                    Err(SubmitError::Retry("buffer full".into())),
                    Ok(future::ready(Ok(())).boxed()),
                ]),
            },
            Duration::from_secs(10),
        );
        let (requests, status) = requests(1);
        stream::iter(requests)
            .into_driver(service)
            .run()
            .await
            .unwrap();
        assert_eq!(status.await, BatchStatus::Delivered);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_delivery_is_errored_and_never_replayed() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let service = FlussService::new(
            FakeSubmitter {
                attempts: Arc::clone(&attempts),
                results: VecDeque::from([Ok(future::ready(Err("server failure".into())).boxed())]),
            },
            Duration::from_secs(10),
        );
        let (requests, status) = requests(1);
        stream::iter(requests)
            .into_driver(service)
            .run()
            .await
            .unwrap();
        assert_eq!(status.await, BatchStatus::Errored);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_timeout_and_cancellation_cannot_acknowledge() {
        for cancel in [false, true] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let (_sender, delivery) = pending_delivery();
            let service = FlussService::new(
                FakeSubmitter {
                    attempts: Arc::clone(&attempts),
                    results: VecDeque::from([Ok(delivery)]),
                },
                Duration::from_secs(10),
            );
            let (requests, mut status) = requests(1);
            let task = tokio::spawn(stream::iter(requests).into_driver(service).run());
            assert!(
                tokio::time::timeout(Duration::from_millis(1), &mut status)
                    .await
                    .is_err()
            );
            if cancel {
                task.abort();
            }
            assert_eq!(status.await, BatchStatus::Errored);
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn malformed_input_is_rejected_without_retry() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let service = FlussService::new(
            FakeSubmitter {
                attempts: Arc::clone(&attempts),
                results: VecDeque::from([Err(SubmitError::Rejected("invalid column".into()))]),
            },
            Duration::from_secs(10),
        );
        let (requests, status) = requests(1);
        stream::iter(requests)
            .into_driver(service)
            .run()
            .await
            .unwrap();
        assert_eq!(status.await, BatchStatus::Rejected);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drains_or_errors_pending_and_queued_events_at_deadline() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (_sender, delivery) = pending_delivery();
        let service = ServiceBuilder::new()
            .concurrency_limit(1)
            .service(FlussService::new(
                FakeSubmitter {
                    attempts,
                    results: VecDeque::from([Ok(delivery)]),
                },
                Duration::from_secs(60),
            ));
        let (requests, status) = requests(3);
        let start = Instant::now();
        let (result, _) = super::super::sink::drive_requests(
            stream::iter(requests),
            service,
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(result, Err(()));
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        assert_eq!(status.await, BatchStatus::Errored);
    }

    #[test]
    fn native_failures_distinguish_bad_input_from_client_failure() {
        assert!(matches!(
            SubmitError::from(Error::BufferExhausted {
                message: "full".into()
            }),
            SubmitError::Retry(_)
        ));
        assert!(matches!(
            SubmitError::from(Error::IllegalArgument {
                message: "bad row".into()
            }),
            SubmitError::Rejected(_)
        ));
        assert!(matches!(
            SubmitError::from(Error::UnexpectedError {
                message: "client failure".into(),
                source: None
            }),
            SubmitError::Unknown(_)
        ));
    }

    #[test]
    fn schema_identity_includes_table_recreation() {
        let before = SchemaIdentity {
            table_id: 1,
            schema_id: 1,
        };
        assert_ne!(
            before,
            SchemaIdentity {
                table_id: 1,
                schema_id: 2
            }
        );
        assert_ne!(
            before,
            SchemaIdentity {
                table_id: 2,
                schema_id: 1
            }
        );
    }
}

#[cfg(test)]
#[path = "integration_tests.rs"]
mod integration_tests;

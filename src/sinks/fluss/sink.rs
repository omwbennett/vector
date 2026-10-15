use std::{sync::Arc, time::Duration};

use futures::stream;
use tokio::{
    sync::oneshot,
    time::{Instant, timeout_at},
};

use super::{
    config::FlussSinkConfig,
    service::{ConnectionSlot, FlussRequest, FlussService, NativeSubmitter},
};
use crate::sinks::prelude::*;

pub struct FlussSink {
    config: FlussSinkConfig,
    table_template: ConfinedTemplate,
}

impl FlussSink {
    pub(super) const fn new(config: FlussSinkConfig, table_template: ConfinedTemplate) -> Self {
        Self {
            config,
            table_template,
        }
    }

    fn request(&self, mut event: Event) -> FlussRequest {
        let table = self
            .table_template
            .render_string(&event)
            .map_err(|error| error.to_string())
            .and_then(|qualified| {
                let name = qualified
                    .strip_prefix(&format!("{}.", self.config.database))
                    .ok_or_else(|| "table escaped configured database".to_owned())?;
                match fluss::metadata::TablePath::detect_invalid_name(name) {
                    Some(error) => Err(error),
                    None => Ok(name.to_owned()),
                }
            });
        if let Some(field) = &self.config.table_field {
            match &mut event {
                Event::Log(log) => {
                    log.remove(&field.0);
                }
                Event::Trace(trace) => {
                    trace.remove(&field.0);
                }
                Event::Metric(_) => {}
            }
        }
        FlussRequest::new(event, table)
    }
}

#[async_trait]
impl StreamSink<Event> for FlussSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let connections = Arc::new(ConnectionSlot::default());
        let service = ServiceBuilder::new()
            .concurrency_limit(self.config.max_in_flight)
            .service(FlussService::new(
                NativeSubmitter::new(self.config.clone(), Arc::clone(&connections)),
                Duration::from_millis(self.config.request_timeout_ms),
            ));
        let shutdown_timeout = Duration::from_millis(self.config.shutdown_timeout_ms);
        let requests = input.map(|event| self.request(event));
        let (result, deadline) = drive_requests(requests, service, shutdown_timeout).await;
        // Closing the shared client wakes a blocked append and fails unresolved native futures.
        connections
            .close(deadline.saturating_duration_since(Instant::now()))
            .await;
        result
    }
}

pub(super) async fn drive_requests<St, S>(
    requests: St,
    service: S,
    shutdown_timeout: Duration,
) -> (Result<(), ()>, Instant)
where
    St: futures::Stream<Item = FlussRequest>,
    S: Service<FlussRequest>,
    S::Error: std::fmt::Debug + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse,
{
    let (eof_tx, eof_rx) = oneshot::channel();
    let requests = requests
        .map(Some)
        .chain(stream::once(async move {
            _ = eof_tx.send(());
            None
        }))
        .filter_map(future::ready);
    let driver = requests.into_driver(service).protocol("fluss").run();
    tokio::pin!(driver);
    tokio::select! {
        result = &mut driver => (result, Instant::now() + shutdown_timeout),
        _ = eof_rx => {
            let deadline = Instant::now() + shutdown_timeout;
            let result = timeout_at(deadline, &mut driver).await.unwrap_or_else(|_| {
                error!(message = "Fluss shutdown deadline expired; pending events have unknown delivery status.");
                Err(())
            });
            (result, deadline)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::service::{Delivery, Submit, SubmitError};
    use super::*;
    use crate::config::ValidatedSink;
    use vector_lib::{
        finalization::{BatchNotifier, BatchStatus},
        lookup::event_path,
    };

    struct CheckRouting;

    impl Submit for CheckRouting {
        fn submit<'a>(
            &'a mut self,
            table: &'a str,
            event: &'a Event,
            _deadline: Instant,
        ) -> BoxFuture<'a, Result<Delivery, SubmitError>> {
            assert_eq!(table, "routed_events");
            assert!(event.as_log().get(event_path!("table")).is_none());
            assert_eq!(
                event.as_log().get(event_path!("message")),
                Some(&Value::from("hello"))
            );
            future::ready(Ok(future::ready(Ok(())).boxed())).boxed()
        }
    }

    #[tokio::test]
    async fn routing_removes_table_field_and_rejects_invalid_destinations() {
        let mut config: FlussSinkConfig =
            serde_json::from_value(FlussSinkConfig::generate_config()).unwrap();
        config.table = Template::try_from("{{ .table }}").unwrap();
        config.table_field = Some("table".into());
        let table_template = config.validate().unwrap();
        let sink = FlussSink::new(config, table_template);
        for (table, expected) in [
            (Some("routed_events"), BatchStatus::Delivered),
            (Some("../other"), BatchStatus::Rejected),
            (None, BatchStatus::Rejected),
        ] {
            let mut event = Event::Log(LogEvent::from("hello"));
            if let Some(table) = table {
                event.as_mut_log().insert(event_path!("table"), table);
            }
            let status = BatchNotifier::apply_to(std::slice::from_mut(&mut event));
            let service = FlussService::new(CheckRouting, Duration::from_secs(1));
            let (result, _) = drive_requests(
                stream::iter([sink.request(event)]),
                service,
                Duration::from_secs(1),
            )
            .await;
            assert_eq!(result, Ok(()));
            assert_eq!(status.await, expected);
        }
    }
}

use fluss::{
    client::{EARLIEST_OFFSET, FlussAdmin},
    metadata::{DataTypes, Schema, TableDescriptor},
    row::DataGetters,
    rpc::message::OffsetSpec,
};

use super::*;

async fn ready(admin: &FlussAdmin, path: &TablePath) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while admin
            .list_offsets(path, &[0], OffsetSpec::Latest)
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("table did not become ready");
}

// Run with FLUSS_TEST_BOOTSTRAP pointing at a disposable Fluss 0.9.1+ cluster.
#[tokio::test]
#[ignore = "requires a disposable Fluss cluster and FLUSS_TEST_BOOTSTRAP"]
async fn native_delivery_recreation_cache_and_close() {
    let mut config: FlussSinkConfig =
        serde_json::from_value(FlussSinkConfig::generate_config()).unwrap();
    config.bootstrap_servers =
        std::env::var("FLUSS_TEST_BOOTSTRAP").expect("set FLUSS_TEST_BOOTSTRAP");
    config.database = format!("vector_sink_{}", uuid::Uuid::new_v4().simple());
    config.max_cached_tables = 1;
    let control = FlussConnection::new(build_fluss_config(&config))
        .await
        .unwrap();
    let admin = control.get_admin().unwrap();
    admin
        .create_database(&config.database, None, false)
        .await
        .unwrap();
    let path = TablePath::new(&config.database, "events");
    let other = TablePath::new(&config.database, "other");
    let descriptor = |wide| {
        TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column(
                        "id",
                        if wide {
                            DataTypes::int()
                        } else {
                            DataTypes::tinyint()
                        },
                    )
                    .column("message", DataTypes::string())
                    .build()
                    .unwrap(),
            )
            .distributed_by(Some(1), vec![])
            .build()
            .unwrap()
    };
    admin
        .create_table(&path, &descriptor(false), false)
        .await
        .unwrap();
    ready(&admin, &path).await;
    let connections = Arc::new(ConnectionSlot::default());
    let mut backend = NativeSubmitter::new(config.clone(), Arc::clone(&connections));
    let mut event = Event::Log(LogEvent::from("hello"));
    event
        .as_mut_log()
        .insert(vector_lib::lookup::event_path!("id"), 1);
    backend
        .try_submit("events", &event, Instant::now() + Duration::from_secs(30))
        .await
        .unwrap()
        .await
        .unwrap();
    let before = backend.writers["events"].identity;
    let old_writer = Arc::clone(&backend.writers["events"].writer);

    admin.drop_table(&path, false).await.unwrap();
    admin
        .create_table(&path, &descriptor(true), false)
        .await
        .unwrap();
    ready(&admin, &path).await;
    event
        .as_mut_log()
        .insert(vector_lib::lookup::event_path!("id"), 300);
    // Conversion fails against the cached TINYINT, forcing one refresh before append.
    backend
        .try_submit("events", &event, Instant::now() + Duration::from_secs(30))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_ne!(backend.writers["events"].identity.table_id, before.table_id);
    assert!(matches!(
        old_writer.append(&fluss::row::GenericRow::new(2)),
        Err(Error::WriterClosed { .. })
    ));

    let table = control.get_table(&path).await.unwrap();
    let scanner = table.new_scan().create_log_scanner().unwrap();
    scanner.subscribe(0, EARLIEST_OFFSET).await.unwrap();
    let records = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let records = scanner.poll(Duration::from_millis(100)).await.unwrap();
            if !records.is_empty() {
                break records;
            }
        }
    })
    .await
    .unwrap();
    let mut records = records.into_iter();
    let record = records.next().unwrap();
    assert!(records.next().is_none());
    assert_eq!(record.row().get_int(0).unwrap(), 300);
    assert_eq!(record.row().get_string(1).unwrap(), "hello");

    admin
        .create_table(&other, &descriptor(true), false)
        .await
        .unwrap();
    ready(&admin, &other).await;
    let previous = connections
        .0
        .lock()
        .expect("connection lock poisoned")
        .clone()
        .unwrap();
    backend
        .try_submit("other", &event, Instant::now() + Duration::from_secs(30))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(backend.writers.len(), 1);
    assert!(backend.writers.contains_key("other"));
    assert!(!Arc::ptr_eq(
        &previous,
        connections
            .0
            .lock()
            .expect("connection lock poisoned")
            .as_ref()
            .unwrap()
    ));
    connections.close(Duration::from_secs(5)).await;
    assert!(
        connections
            .0
            .lock()
            .expect("connection lock poisoned")
            .is_none()
    );
    assert!(matches!(
        backend.writers["other"]
            .writer
            .append(&fluss::row::GenericRow::new(2)),
        Err(Error::WriterClosed { .. })
    ));
    admin
        .drop_database(&config.database, false, true)
        .await
        .unwrap();
    control.close(Duration::from_secs(5)).await.unwrap();
}

mod common;

use common::{Event, SourceId, StreamToVec};
use eventually::store::{EventStore, Expected, Persisted, Select};
use eventually::versioning::Versioned;
use eventually_event_store_db::{BuilderError, EventStoreBuilder, GenericEvent};
use futures::stream::StreamExt;

#[tokio::test]
async fn event_store_db_verify_connection_valid() {
    EventStoreBuilder::new("esdb://localhost:2113?tls=false")
        .await
        .unwrap()
        .verify_connection(3)
        .await
        .unwrap();
}

#[tokio::test]
async fn event_store_db_verify_connection_invalid() {
    let verify = EventStoreBuilder::new("esdb://localhost:1111?tls=false")
        .await
        .unwrap()
        .verify_connection(3)
        .await;

    match verify.unwrap_err() {
        BuilderError::VerificationTimeout => {}
        _ => panic!("expected connection timeout verification error"),
    }
}

#[tokio::test]
async fn event_store_db_read_write() {
    let mut client = EventStoreBuilder::new("esdb://localhost:2113?tls=false")
        .await
        .unwrap()
        .build_store();

    // Start from scratch.
    client.remove(SourceId::Foo).await.unwrap();
    client.remove(SourceId::Bar).await.unwrap();

    // Write a single event.
    client
        .append(SourceId::Foo, Expected::Any, vec![Event::one()])
        .await
        .unwrap();

    // Write multiple events.
    client
        .append(
            SourceId::Foo,
            Expected::Any,
            vec![Event::two(), Event::three(), Event::four()],
        )
        .await
        .unwrap();

    // Read events from stream.
    let foo_events = client.stream(SourceId::Foo, Select::All).to_vec().await;
    assert_eq!(foo_events.len(), 4);
    assert_eq!(foo_events[0], Event::one());
    assert_eq!(foo_events[1], Event::two());
    assert_eq!(foo_events[2], Event::three());
    assert_eq!(foo_events[3], Event::four());

    // Read events from empty "bar" stream ID.
    let bar_events = client.stream(SourceId::Bar, Select::All).to_vec().await;
    assert!(bar_events.is_empty());

    // Write multiple events to empty "bar" stream ID.
    client
        .append(
            SourceId::Bar,
            Expected::Any,
            vec![Event::four(), Event::three(), Event::three()],
        )
        .await
        .unwrap();

    // Read events from empty "bar" stream ID.
    let bar_events = client.stream(SourceId::Bar, Select::All).to_vec().await;
    assert_eq!(bar_events.len(), 3);
    assert_eq!(bar_events[0], Event::four());
    assert_eq!(bar_events[1], Event::three());
    assert_eq!(bar_events[2], Event::three());

    // Verify: Read events from "foo" stream ID (must not change).
    let foo_events = client.stream(SourceId::Foo, Select::All).to_vec().await;
    assert_eq!(foo_events.len(), 4);
    assert_eq!(foo_events[0], Event::one());
    assert_eq!(foo_events[1], Event::two());
    assert_eq!(foo_events[2], Event::three());
    assert_eq!(foo_events[3], Event::four());

    // Read **all** events.
    let all_events = client.stream_all(Select::All).to_vec().await;
    assert!(all_events.len() >= foo_events.len() + bar_events.len());
    assert!(all_events.contains(&Event::one()));
    assert!(all_events.contains(&Event::two()));
    assert!(all_events.contains(&Event::three()));
    assert!(all_events.contains(&Event::four()));

    // Stream from a specific version.
    let persisted_foo_events = client
        .stream(SourceId::Foo, Select::All)
        .await
        .unwrap()
        .map(|persisted| persisted.unwrap())
        .collect::<Vec<Persisted<SourceId, GenericEvent>>>()
        .await;

    let at = persisted_foo_events[2].version();

    let foo_events = client
        .stream(SourceId::Foo, Select::From(at))
        .to_vec()
        .await;
    assert_eq!(foo_events.len(), 2);
    assert_eq!(foo_events[0], Event::three());
    assert_eq!(foo_events[1], Event::four());

    // Append from a specific version.
    let at = persisted_foo_events[3].version();

    // Version does not exist
    let res = client
        .append(
            SourceId::Foo,
            Expected::Exact(at + 1),
            vec![Event::two(), Event::two()],
        )
        .await;
    assert!(res.is_err());

    // Version **does** not exist
    client
        .append(
            SourceId::Foo,
            Expected::Exact(at),
            vec![Event::two(), Event::two()],
        )
        .await
        .unwrap();

    let foo_events = client.stream(SourceId::Foo, Select::All).to_vec().await;
    assert_eq!(foo_events.len(), 6);
    assert_eq!(foo_events[0], Event::one());
    assert_eq!(foo_events[1], Event::two());
    assert_eq!(foo_events[2], Event::three());
    assert_eq!(foo_events[3], Event::four());
    assert_eq!(foo_events[4], Event::two());
    assert_eq!(foo_events[5], Event::two());

    // Cleanup
    client.remove(SourceId::Foo).await.unwrap();
    client.remove(SourceId::Bar).await.unwrap();

    // Verify cleanup
    let foo_events = client.stream(SourceId::Foo, Select::All).to_vec().await;
    assert!(foo_events.is_empty());

    let bar_events = client.stream(SourceId::Bar, Select::All).to_vec().await;
    assert!(bar_events.is_empty());
}

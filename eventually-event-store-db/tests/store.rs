#[macro_use]
extern crate serde;

use eventually::store::{EventStore, Expected, Persisted, Select};
use eventually_event_store_db::{BuilderError, EventStoreBuilder, StoreError};
use futures::stream::StreamExt;
use std::fmt;
use std::{convert::TryFrom, vec};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Event {
    name: String,
    data: u32,
}

impl Event {
    fn one() -> Self {
        Event {
            name: String::from("Event One"),
            data: 1,
        }
    }
    fn two() -> Self {
        Event {
            name: String::from("Event Two"),
            data: 2,
        }
    }
    fn three() -> Self {
        Event {
            name: String::from("Event Three"),
            data: 3,
        }
    }
    fn four() -> Self {
        Event {
            name: String::from("Event Four"),
            data: 4,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
enum SourceId {
    Foo,
    Bar,
}

impl TryFrom<String> for SourceId {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "foo" => Ok(SourceId::Foo),
            "bar" => Ok(SourceId::Bar),
            _ => Err(()),
        }
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", {
            match self {
                SourceId::Foo => "foo",
                SourceId::Bar => "bar",
            }
        })
    }
}

#[tokio::test]
async fn event_store_db_verify_connection_valid() {
    let verify = EventStoreBuilder::new("esdb://localhost:2113?tls=false")
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
    fn from_stream() {}

    let mut client = EventStoreBuilder::new("esdb://localhost:2113?tls=false")
        .await
        .unwrap()
        .build_store();

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
    let events = client
        .stream(SourceId::Foo, Select::All)
        .await
        .unwrap()
        .collect::<Vec<Result<Persisted<SourceId, Event>, StoreError>>>()
        .await
        .into_iter()
        .map(|persisted| persisted.unwrap().take())
        .collect::<Vec<Event>>();

    assert_eq!(events.len(), 4);
    assert_eq!(events[0], Event::one());
    assert_eq!(events[1], Event::two());
    assert_eq!(events[2], Event::three());
    assert_eq!(events[3], Event::four());

    // Read events from empty "bar" stream ID.
    let events = client
        .stream(SourceId::Bar, Select::All)
        .await
        .unwrap()
        .collect::<Vec<Result<Persisted<SourceId, Event>, StoreError>>>()
        .await
        .into_iter()
        .map(|persisted| persisted.unwrap().take())
        .collect::<Vec<Event>>();

    assert!(events.is_empty());

    // Write multiple events to empty "bar" stream ID.
    client
        .append(
            SourceId::Bar,
            Expected::Any,
            vec![Event::four(), Event::three()],
        )
        .await
        .unwrap();

    // Read events from empty "bar" stream ID.
    let events = client
        .stream(SourceId::Bar, Select::All)
        .await
        .unwrap()
        .collect::<Vec<Result<Persisted<SourceId, Event>, StoreError>>>()
        .await
        .into_iter()
        .map(|persisted| persisted.unwrap().take())
        .collect::<Vec<Event>>();

    // Read events from "bar" stream ID.
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], Event::four());
    assert_eq!(events[1], Event::three());

    // Verify: Read events from "foo" stream ID (must not change).
    let events = client
        .stream(SourceId::Foo, Select::All)
        .await
        .unwrap()
        .collect::<Vec<Result<Persisted<SourceId, Event>, StoreError>>>()
        .await
        .into_iter()
        .map(|persisted| persisted.unwrap().take())
        .collect::<Vec<Event>>();

    assert_eq!(events.len(), 4);
    assert_eq!(events[0], Event::one());
    assert_eq!(events[1], Event::two());
    assert_eq!(events[2], Event::three());
    assert_eq!(events[3], Event::four());

    // Cleanup
    client.remove(SourceId::Foo).await.unwrap();
    client.remove(SourceId::Bar).await.unwrap();

    // Verify cleanup

    let events = client
        .stream(SourceId::Foo, Select::All)
        .await
        .unwrap()
        .collect::<Vec<Result<Persisted<SourceId, Event>, StoreError>>>()
        .await
        .into_iter()
        .map(|persisted| persisted.unwrap().take())
        .collect::<Vec<Event>>();

    assert!(events.is_empty());

    let events = client
        .stream(SourceId::Bar, Select::All)
        .await
        .unwrap()
        .collect::<Vec<Result<Persisted<SourceId, Event>, StoreError>>>()
        .await
        .into_iter()
        .map(|persisted| persisted.unwrap().take())
        .collect::<Vec<Event>>();

    // Read events from "bar" stream ID.
    assert!(events.is_empty());
}

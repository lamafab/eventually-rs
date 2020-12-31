#[macro_use]
extern crate serde;
#[macro_use]
extern crate async_trait;

use eventually::store::{EventStore, EventStream, Expected, Persisted, Select};
use eventually_event_store_db::{
    BuilderError, EventStore as EventStoreDB, EventStoreBuilder, StoreError,
};
use futures::future::BoxFuture;
use futures::stream::{Stream, StreamExt};
use serde::de::{Deserialize, DeserializeOwned, Deserializer};
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

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
    /// EventStoreDB has additional, default streams.
    Unknown,
}

impl TryFrom<String> for SourceId {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "foo" => Ok(SourceId::Foo),
            "bar" => Ok(SourceId::Bar),
            _ => Ok(SourceId::Unknown),
        }
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", {
            match self {
                SourceId::Foo => "foo",
                SourceId::Bar => "bar",
                _ => unimplemented!(),
            }
        })
    }
}

/// Convenience implementation
#[async_trait]
trait StreamToVec<T> {
    async fn to_vec(self) -> Vec<T>;
}

#[async_trait]
impl<'a, T: 'static + Send + Sync + Serialize + DeserializeOwned> StreamToVec<T>
    for BoxFuture<'a, Result<EventStream<'a, EventStoreDB<SourceId, T>>, StoreError>>
{
    async fn to_vec(self) -> Vec<T> {
        self.await
            .unwrap()
            .collect::<Vec<Result<Persisted<SourceId, T>, StoreError>>>()
            .await
            .into_iter()
            .map(|persisted| persisted.unwrap().take())
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
struct AnyValue(#[serde(deserialize_with = "handle_empty")] ());

fn handle_empty<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    let s = Vec::<u8>::deserialize(deserializer);
    Ok(())
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
    let events = client.stream(SourceId::Foo, Select::All).to_vec().await;
    assert_eq!(events.len(), 4);
    assert_eq!(events[0], Event::one());
    assert_eq!(events[1], Event::two());
    assert_eq!(events[2], Event::three());
    assert_eq!(events[3], Event::four());

    // Read events from empty "bar" stream ID.
    let events = client.stream(SourceId::Bar, Select::All).to_vec().await;
    assert!(events.is_empty());

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
    let events = client.stream(SourceId::Bar, Select::All).to_vec().await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], Event::four());
    assert_eq!(events[1], Event::three());
    assert_eq!(events[2], Event::three());

    // Verify: Read events from "foo" stream ID (must not change).
    let events = client.stream(SourceId::Foo, Select::All).to_vec().await;

    assert_eq!(events.len(), 4);
    assert_eq!(events[0], Event::one());
    assert_eq!(events[1], Event::two());
    assert_eq!(events[2], Event::three());
    assert_eq!(events[3], Event::four());

    // Read events from **all** streams.
    {
        // Create a temporary scope in order to use the `Option<serde_json::Value>` type
        // instead of `Event`. EventStoreDB has additional, default streams.
        let mut client = EventStoreBuilder::new("esdb://localhost:2113?tls=false")
            .await
            .unwrap()
            .build_store::<SourceId, AnyValue>();

        let events = client.stream_all(Select::All).to_vec().await;
        assert!(events.len() >= 7);
    }

    // Cleanup
    client.remove(SourceId::Foo).await.unwrap();
    client.remove(SourceId::Bar).await.unwrap();

    // Verify cleanup
    let events = client.stream(SourceId::Foo, Select::All).to_vec().await;
    assert!(events.is_empty());

    let events = client.stream(SourceId::Bar, Select::All).to_vec().await;
    assert!(events.is_empty());
}

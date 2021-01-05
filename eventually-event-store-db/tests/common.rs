use eventually::store::EventStream;
use eventually_event_store_db::{EventStore as EventStoreDB, GenericEvent, StoreError};
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt;

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct Event {
    name: String,
    data: u32,
}

impl Event {
    pub fn one() -> GenericEvent {
        GenericEvent::serialize(Event {
            name: String::from("Event One"),
            data: 1,
        })
        .unwrap()
    }
    pub fn two() -> GenericEvent {
        GenericEvent::serialize(Event {
            name: String::from("Event Two"),
            data: 2,
        })
        .unwrap()
    }
    pub fn three() -> GenericEvent {
        GenericEvent::serialize(Event {
            name: String::from("Event Three"),
            data: 3,
        })
        .unwrap()
    }
    pub fn four() -> GenericEvent {
        GenericEvent::serialize(Event {
            name: String::from("Event Four"),
            data: 4,
        })
        .unwrap()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SourceId {
    Foo,
    Bar,
    Baz,
    Fum,
    Grunt,
    /// EventStoreDB has additional, default streams.
    Unknown,
}

impl TryFrom<String> for SourceId {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "foo" => Ok(SourceId::Foo),
            "bar" => Ok(SourceId::Bar),
            "baz" => Ok(SourceId::Baz),
            "fum" => Ok(SourceId::Fum),
            "grunt" => Ok(SourceId::Grunt),
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
                SourceId::Baz => "baz",
                SourceId::Fum => "fum",
                SourceId::Grunt => "grunt",
                _ => unimplemented!(),
            }
        })
    }
}

/// Convenience implementation for converting a stream into a vec.
#[async_trait::async_trait]
pub trait StreamToVec {
    async fn to_vec(self) -> Vec<GenericEvent>;
}

#[async_trait::async_trait]
impl<'a> StreamToVec
    for BoxFuture<'a, Result<EventStream<'a, EventStoreDB<SourceId>>, StoreError>>
{
    async fn to_vec(self) -> Vec<GenericEvent> {
        self.await
            .unwrap()
            .map(|persisted| persisted.unwrap().take())
            .collect()
            .await
    }
}

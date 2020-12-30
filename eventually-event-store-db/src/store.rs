use eventstore::prelude::{
    CurrentRevision, Error as EsError, EventData, ExpectedRevision, ExpectedVersion,
    WrongExpectedVersion,
};
use eventstore::Client as EsClient;
use eventually::store::Expected;
// TODO: Alias `EventStream` as `StoreEventStream`
use eventually::store::{AppendError, EventStream, Select};
use futures::future::BoxFuture;
use serde::ser::Serialize;
use std::fmt;
use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

// TODO: Rename to `StoreResult`?
type Result<T> = std::result::Result<T, StoreError>;

///
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("EventStoreDB error: {0}")]
    EventStoreDb(
        #[source]
        #[from]
        EsError,
    ),
    // TODO: Prettify this
    #[error("Wrong event version, current: {:?}, expected: {:?}", .0.current, .0.expected)]
    UnexpectedVersion(
        #[source]
        #[from]
        WrongExpectedVersion,
    ),
}

// TODO: Clarify this
impl AppendError for StoreError {
    #[inline]
    fn is_conflict_error(&self) -> bool {
        false
    }
}

pub struct EventStore<Id, Event> {
    client: EsClient,
    _p1: PhantomData<Id>,
    _p2: PhantomData<Event>,
}

impl<Id, Event> eventually::EventStore for EventStore<Id, Event>
where
    Id: Send + Eq + Display,
    Event: 'static + Sync + Send + Serialize,
{
    type SourceId = Id;
    type Event = Event;
    type Error = StoreError;

    fn append(
        &mut self,
        source_id: Self::SourceId,
        version: Expected,
        events: Vec<Self::Event>,
    ) -> BoxFuture<Result<u32>> {
        let fut = async move {
            let next_version = self
                .client
                .write_events(format!("{}", source_id))
                .expected_version({
                    match version {
                        Expected::Any => ExpectedVersion::Any,
                        Expected::Exact(v) => ExpectedVersion::Exact(v as u64),
                    }
                })
                .send_iter(
                    events
                        .into_iter()
                        .map(|event| EventData::json("", event).unwrap()),
                )
                .await??
                .next_expected_version;

            // TODO: What if it overflows?
            Ok(next_version as u32)
        };

        Box::pin(fut)
    }

    fn stream(
        &self,
        source_id: Self::SourceId,
        select: Select,
    ) -> BoxFuture<Result<EventStream<Self>>> {
        unimplemented!()
    }

    fn stream_all(&self, select: Select) -> BoxFuture<Result<EventStream<Self>>> {
        unimplemented!()
    }

    fn remove(&mut self, _id: Self::SourceId) -> BoxFuture<Result<()>> {
        unimplemented!()
    }
}

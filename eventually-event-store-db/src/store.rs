use eventstore::prelude::{
    CurrentRevision, Error as EsError, EventData, ExpectedRevision, ExpectedVersion, ReadResult,
    ResolvedEvent, WrongExpectedVersion,
};
use eventstore::Client as EsClient;
use eventually::store::{AppendError, EventStream, Expected, Persisted, Select};
// TODO: Alias `EventStream` as `StoreEventStream`
use futures::future::BoxFuture;
use futures::stream::{Stream, StreamExt};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::error::Error as SerdeError;
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
    #[error("wrong event version (current: {:?}, expected: {:?})", .0.current, .0.expected)]
    UnexpectedVersion(
        #[source]
        #[from]
        WrongExpectedVersion,
    ),
    #[error("specified stream was not found: {0}")]
    StreamNotFound(String),
    // TODO: Consider changed this to `u64`
    #[error("failed to serialize event: {0}")]
    FailedEventSer(
        #[source]
        #[from]
        SerdeError,
    ),
    #[error("failed to deserialize event (event version: {}): {}", .version, .serde_err)]
    FailedEventDes {
        version: u32,
        #[source]
        serde_err: SerdeError,
    },
}

impl AppendError for StoreError {
    fn is_conflict_error(&self) -> bool {
        match self {
            StoreError::UnexpectedVersion(_) => true,
            _ => false,
        }
    }
}

pub struct EventStore<Id, Event> {
    client: EsClient,
    _p1: PhantomData<Id>,
    _p2: PhantomData<Event>,
}

impl<Id, Event> eventually::EventStore for EventStore<Id, Event>
where
    Id: Send + Sync + Eq + Display + Clone,
    Event: 'static + Sync + Send + Serialize + DeserializeOwned,
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
                        .map(|event| {
                            EventData::json("", event)
                                .map_err(|err| StoreError::FailedEventSer(err))
                        })
                        .collect::<Result<Vec<EventData>>>()?,
                )
                .await??
                .next_expected_version;

            // TODO: What if it overflows?
            // TODO: Should this be current version or next expected version?
            Ok(next_version as u32)
        };

        Box::pin(fut)
    }

    fn stream(
        &self,
        source_id: Self::SourceId,
        select: Select,
    ) -> BoxFuture<Result<EventStream<Self>>> {
        let fut = async move {
            self.client
                .read_stream(format!("{}", source_id))
                .start_from({
                    match select {
                        Select::All => 0,
                        Select::From(v) => v as u64,
                    }
                })
                // TODO: `read_through` or `execute`?
                .read_through()
                .await
                .map(|read_res| {
                    let stream = match read_res {
                        ReadResult::Ok(s) => s,
                        ReadResult::StreamNotFound(name) => {
                            return Err(StoreError::StreamNotFound(name))
                        }
                    };

                    Ok(stream.map(move |resolved| {
                        // TODO: Clarify in what cases `event` might be `None`.
                        let event = resolved?.event.unwrap();

                        Ok(Persisted::from(
                            source_id.clone(),
                            serde_json::from_slice::<Event>(event.data.as_ref()).map_err(
                                |err| StoreError::FailedEventDes {
                                    version: event.revision as u32,
                                    serde_err: err,
                                },
                            )?,
                        )
                        .version(event.revision as u32)
                        .sequence_number(0))
                    }))
                })?
                .map(|stream| stream.boxed())
        };

        Box::pin(fut)
    }

    fn stream_all(&self, select: Select) -> BoxFuture<Result<EventStream<Self>>> {
        unimplemented!()
    }

    fn remove(&mut self, _id: Self::SourceId) -> BoxFuture<Result<()>> {
        unimplemented!()
    }
}

impl<Id, Event> EventStore<Id, Event>
where
    Id: Send + Sync + Eq + Display + Clone,
    Event: 'static + Sync + Send + Serialize + DeserializeOwned,
{
}

fn process_stream<Id, Event>(
    id: Id,
    stream: Box<dyn Stream<Item = Result<ResolvedEvent>> + Send + Unpin>,
) -> BoxFuture<'static, Result<EventStream<'static, EventStore<Id, Event>>>>
where
    Id: 'static + Send + Sync + Eq + Display + Clone,
    Event: 'static + Sync + Send + Serialize + DeserializeOwned,
{
    Box::pin(async move {
        Ok(stream
            .map(move |resolved| {
                // TODO: Clarify in what cases `event` might be `None`.
                let event = resolved?.event.unwrap();

                Ok(Persisted::from(
                    id.clone(),
                    serde_json::from_slice::<Event>(event.data.as_ref()).map_err(|err| {
                        StoreError::FailedEventDes {
                            version: event.revision as u32,
                            serde_err: err,
                        }
                    })?,
                )
                .version(event.revision as u32)
                .sequence_number(0))
            })
            .boxed())
    })
}

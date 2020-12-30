use eventstore::prelude::{
    Error as EsError, EventData, ExpectedVersion, Position, ReadResult, ResolvedEvent,
    WrongExpectedVersion,
};
use eventstore::Client as EsClient;
use eventually::store::{AppendError, EventStream, Expected, Persisted, Select};
// TODO: Alias `EventStream` as `StoreEventStream`
use futures::future::BoxFuture;
use futures::stream::{empty as empty_stream, Stream, StreamExt};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::error::Error as SerdeError;
use std::convert::TryFrom;
use std::fmt::Display;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time;

// TODO: Rename to `StoreResult`?
type Result<T> = std::result::Result<T, StoreError>;

/// TODO
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
    // TODO: Remove this?
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
    #[error("failed to convert stream ID to source id: {0}")]
    FailedStreamIdConv(String),
}

impl AppendError for StoreError {
    fn is_conflict_error(&self) -> bool {
        match self {
            StoreError::UnexpectedVersion(_) => true,
            _ => false,
        }
    }
}

/// TODO
pub struct EventStore<Id, Event> {
    client: EsClient,
    _p1: PhantomData<Id>,
    _p2: PhantomData<Event>,
}

impl<Id, Event> EventStore<Id, Event> {
    pub(super) fn new(client: EsClient) -> EventStore<Id, Event> {
        EventStore {
            client: client,
            _p1: PhantomData,
            _p2: PhantomData,
        }
    }
    #[cfg(feature = "verify-connection")]
    pub(super) async fn verify_connection(client: &EsClient, timeout: u64) -> Result<()> {
        time::timeout(Duration::from_secs(timeout), async move {
            // Attempt to read from stream ID. It's irrelevant whether the
            // stream ID exists or not.
            let _ = client
                .read_stream("eventually-init-verification")
                .read_through()
                .await?;

            Ok(())
        })
        .await
        // The precise error type (`BuilderError::VerificationTimeout`) is
        // invoked by the builder, so just use a filler here.
        .map_err(|_| StoreError::StreamNotFound(String::new()))?
    }
}

impl<Id, Event> eventually::EventStore for EventStore<Id, Event>
where
    Id: 'static + Send + Sync + Eq + Display + Clone + TryFrom<String>,
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
                            EventData::json("some-type", event)
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
                        // The Rust client for EventStoreDB returns an error if
                        // the stream ID does not exists, so just return an
                        // empty stream. Events might be added later to it
                        // (which therefore creates the stream ID).
                        ReadResult::StreamNotFound(_) => return Ok(empty_stream().boxed()),
                    };

                    process_stream(stream)
                })?
        };

        Box::pin(fut)
    }

    fn stream_all(&self, select: Select) -> BoxFuture<Result<EventStream<Self>>> {
        let fut = async move {
            self.client
                .read_all()
                .start_from({
                    match select {
                        Select::All => Position::start(),
                        Select::From(v) => Position {
                            commit: v as u64,
                            prepare: v as u64,
                        },
                    }
                })
                // TODO: `read_through` or `execute`?
                .read_through()
                .await
                .map(|stream| process_stream(stream))?
        };

        Box::pin(fut)
    }

    fn remove(&mut self, id: Self::SourceId) -> BoxFuture<Result<()>> {
        let fut = async move {
            Ok(self
                .client
                .delete_stream(format!("{}", id))
                .soft_delete()
                .execute()
                .await
                .map(|_| ())?)
        };

        Box::pin(fut)
    }
}

fn process_stream<Id, Event>(
    stream: Box<dyn Stream<Item = std::result::Result<ResolvedEvent, EsError>> + Send + Unpin>,
) -> Result<EventStream<'static, EventStore<Id, Event>>>
where
    Id: 'static + Send + Sync + Eq + Display + Clone + TryFrom<String>,
    Event: 'static + Sync + Send + Serialize + DeserializeOwned,
{
    Ok(stream
        .map(move |resolved| {
            // TODO: Clarify in what cases `event` might be `None`.
            let mut event = resolved?.event.unwrap();

            let stream_id = std::mem::take(&mut event.stream_id);
            Ok(Persisted::from(
                Id::try_from(stream_id.clone())
                    .map_err(|_| StoreError::FailedStreamIdConv(stream_id))?,
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
}

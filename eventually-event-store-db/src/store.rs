use eventually::store::Expected;
// TODO: Alias `EventStream` as `StoreEventStream`
use eventually::store::{AppendError, EventStream, Select};
use futures::future::BoxFuture;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

// TODO: Rename to `StoreResult`?
type Result<T> = std::result::Result<T, StoreError>;

///
#[derive(Debug, thiserror::Error)]
pub enum StoreError {}

// TODO: Clarify this
impl AppendError for StoreError {
    #[inline]
    fn is_conflict_error(&self) -> bool {
        false
    }
}

pub struct EventStore<Id, Event> {
    _p1: PhantomData<Id>,
    _p2: PhantomData<Event>,
}

impl<Id, Event> eventually::EventStore for EventStore<Id, Event>
where
    Id: Eq,
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
        unimplemented!()
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

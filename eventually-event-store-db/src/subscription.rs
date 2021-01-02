use super::store::{process_stream, StoreError};
use eventually::subscription::{EventStream, Subscription, SubscriptionStream};
use futures::future::BoxFuture;
use std::marker::PhantomData;

pub struct EventSubscriber<Id, Event> {
    client: eventstore::Client,
    _p1: PhantomData<Id>,
    _p2: PhantomData<Event>,
}

impl<Id, Event> Subscription for EventSubscriber<Id, Event>
where
    Id: Eq,
{
    type SourceId = Id;
    type Event = Event;
    type Error = StoreError;

    fn resume(&self) -> BoxFuture<Result<SubscriptionStream<Self>, Self::Error>> {
        unimplemented!()
    }

    fn checkpoint(&self, version: u32) -> BoxFuture<Result<(), Self::Error>> {
        unimplemented!()
    }
}

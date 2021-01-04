use super::store::{process_stream, StoreError};
use super::GenericEvent;
use eventually::subscription::EventStream;
use futures::future::BoxFuture;
use std::convert::TryFrom;
use std::fmt::Display;
use std::marker::PhantomData;

/// TODO
pub struct EventSubscriber<Id> {
    client: eventstore::Client,
    _p1: PhantomData<Id>,
}

impl<Id> EventSubscriber<Id> {
    pub(super) fn new(client: eventstore::Client) -> Self {
        EventSubscriber {
            client: client,
            _p1: PhantomData,
        }
    }
}

impl<Id> eventually::EventSubscriber for EventSubscriber<Id>
where
    Id: 'static + Send + Sync + Eq + Display + TryFrom<String> + Clone,
{
    type SourceId = Id;
    type Event = GenericEvent;
    type Error = StoreError;

    fn subscribe_all(&self) -> BoxFuture<Result<EventStream<Self>, Self::Error>> {
        let fut = async move {
            self.client
                .subscribe_to_all_from()
                .execute_event_appeared_only()
                .await
                .map(|stream| process_stream(stream))?
        };

        Box::pin(fut)
    }
}

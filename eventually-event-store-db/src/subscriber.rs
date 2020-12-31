use super::store::{process_stream, StoreError};
use eventually::subscription::EventStream;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt::Display;
use std::marker::PhantomData;

/// TODO
pub struct EventSubscriber<Id, Event> {
    client: eventstore::Client,
    _p1: PhantomData<Id>,
    _p2: PhantomData<Event>,
}

impl<Id, Event> eventually::EventSubscriber for EventSubscriber<Id, Event>
where
    Id: 'static + Send + Sync + Eq + Display + TryFrom<String> + Clone,
    Event: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    type SourceId = Id;
    type Event = Event;
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

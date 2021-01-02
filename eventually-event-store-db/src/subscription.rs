use super::store::{process_stream, StoreError};
use eventstore::PersistentSubscriptionSettings;
use eventually::subscription::{EventStream, Subscription, SubscriptionStream};
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Display;
use std::marker::PhantomData;

pub struct EventSubscription<Id, Event> {
    client: eventstore::Client,
    stream_id: &'static str,
    subscription_name: &'static str,
    _p1: PhantomData<Id>,
    _p2: PhantomData<Event>,
}

impl<Id, Event> Subscription for EventSubscription<Id, Event>
where
    Id: 'static + Send + Sync + Eq + Display + Clone,
    Event: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    type SourceId = Id;
    type Event = Event;
    type Error = StoreError;

    fn resume(&self) -> BoxFuture<Result<SubscriptionStream<Self>, Self::Error>> {
        unimplemented!()
    }

    fn checkpoint(&self, version: u32) -> BoxFuture<Result<(), Self::Error>> {
        let fut = async move {
            self.client
                .update_persistent_subscription(self.stream_id, self.subscription_name)
                .execute({
                    let mut settings = PersistentSubscriptionSettings::default();
                    settings.revision = version as u64;
                    // TODO: Verify this:
                    settings.max_checkpoint_count = 0;
                    settings
                })
                .await
                .map_err(|err| err.into())
        };

        Box::pin(fut)
    }
}

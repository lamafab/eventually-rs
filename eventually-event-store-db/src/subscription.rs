use super::store::StoreError;
use super::GenericEvent;
use eventstore::PersistentSubscriptionSettings;
use eventually::store::Persisted;
use eventually::subscription::{Subscription, SubscriptionStream};
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use serde::de::DeserializeOwned;
use std::convert::TryFrom;
use std::marker::PhantomData;

pub struct EventSubscription<Id> {
    client: eventstore::Client,
    stream_id: &'static str,
    subscription_name: &'static str,
    _p1: PhantomData<Id>,
}

impl<Id> Subscription for EventSubscription<Id>
where
    Id: 'static + Send + Sync + Eq + TryFrom<String> + Clone,
{
    type SourceId = Id;
    type Event = GenericEvent;
    type Error = StoreError;

    fn resume(&self) -> BoxFuture<Result<SubscriptionStream<Self>, Self::Error>> {
        let fut = async move {
            let (mut tx, rx) = mpsc::unbounded();

            self.client
                .connect_persistent_subscription(self.stream_id, self.subscription_name)
                .execute()
                .await
                .map(|(mut read, mut write)| async move {
                    while let Some(resolved) = read.try_next_event().await? {
                        // TODO: Clarify in what cases `event` might be `None`.
                        let mut event = resolved.event.unwrap();

                        let stream_id = std::mem::take(&mut event.stream_id);
                        tx.start_send(Ok(Persisted::from(
                            Id::try_from(stream_id.clone())
                                .map_err(|_| StoreError::FailedStreamIdConv(stream_id))?,
                            GenericEvent::from(event.data),
                        )
                        .version(event.revision as u32)
                        .sequence_number(0)))
                            .map_err(|_| StoreError::FailedToProcessEvent)?;

                        write.ack(vec![event.id]).await.unwrap();
                    }

                    #[allow(unused_qualifications)]
                    Result::<(), StoreError>::Ok(())
                })?
                .await?;

            Ok(rx.boxed())
        };

        Box::pin(fut)
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

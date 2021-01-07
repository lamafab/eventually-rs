use super::store::StoreError;
use super::GenericEvent;
use eventstore::prelude::{SubEvent, SubscriptionRead, SubscriptionWrite};
use eventstore::{PersistentSubscriptionSettings, ResolvedEvent};
use eventually::store::Persisted;
use eventually::subscription::{Subscription, SubscriptionStream};
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::stream::{Stream, StreamExt};
use futures::task::{Context, Poll};
use std::convert::TryFrom;
use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use uuid::Uuid;

/// TODO
pub struct EventSubscription<Id> {
    client: eventstore::Client,
    source_id: Id,
    subscription_name: &'static str,
    _p1: PhantomData<Id>,
}

impl<Id> EventSubscription<Id> {
    pub(super) fn new(
        client: eventstore::Client,
        source_id: Id,
        subscription_name: &'static str,
    ) -> Self {
        EventSubscription {
            client: client,
            source_id: source_id,
            subscription_name: subscription_name,
            _p1: PhantomData,
        }
    }
}

/// TODO
pub struct PersistentStream<Id> {
    reader: SubscriptionRead,
    writer: SubscriptionWrite,
    _p1: PhantomData<Id>,
}

impl<Id> Unpin for PersistentStream<Id> {}

impl<Id> Stream for PersistentStream<Id>
where
    Id: TryFrom<String>,
{
    type Item = Result<Persisted<Id, GenericEvent>, StoreError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        fn x<Id>(resolved: ResolvedEvent) -> Result<(Persisted<Id, GenericEvent>, Uuid), StoreError>
        where
            Id: TryFrom<String>,
        {
            let mut event = resolved.event.unwrap();
            let uuid = event.id;
            let stream_id = std::mem::take(&mut event.stream_id);

            Ok((
                Persisted::from(
                    Id::try_from(stream_id.clone())
                        .map_err(|_| StoreError::FailedStreamIdConv(stream_id))?,
                    GenericEvent::from(event.data),
                )
                .version(event.revision as u32)
                .sequence_number(0),
                uuid,
            ))
        }

        let event = match Box::pin(self.as_mut().reader.try_next_event())
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(event) => event,
            Poll::Pending => return Poll::Pending,
        };

        let processed = x::<Id>(event.unwrap().unwrap());

        if let Ok((_, uuid)) = processed {
            match Box::pin(self.as_mut().writer.ack(vec![uuid]))
                .as_mut()
                .poll(cx)
            {
                Poll::Ready(_) => {}
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Ready(Some(processed.map(|(persisted, _)| persisted)))
    }
}

impl<Id> Subscription for EventSubscription<Id>
where
    Id: 'static + Send + Sync + Eq + TryFrom<String> + Clone + Display,
{
    type SourceId = Id;
    type Event = GenericEvent;
    type Error = StoreError;

    fn resume(&self) -> BoxFuture<Result<SubscriptionStream<Self>, Self::Error>> {
        let fut = async move {
            // TODO: add trait bound for `AsRef<str>` instead of `Display`
            self.client
                .delete_persistent_subscription(
                    self.source_id.to_string().as_str(),
                    self.subscription_name,
                )
                .execute()
                .await
                .unwrap();

            self.client
                .create_persistent_subscription(
                    self.source_id.to_string().as_str(),
                    self.subscription_name,
                )
                .execute(PersistentSubscriptionSettings::default())
                .await?;

            Ok(self
                .client
                .connect_persistent_subscription(
                    self.source_id.to_string().as_str(),
                    self.subscription_name,
                )
                .execute()
                .await
                .map(|(mut read, mut write)| async move {
                    PersistentStream {
                        reader: read,
                        writer: write,
                        _p1: PhantomData,
                    }
                })?
                .await
                .boxed())
        };

        Box::pin(fut)
    }

    fn checkpoint(&self, version: u32) -> BoxFuture<Result<(), Self::Error>> {
        let fut = async move {
            self.client
                .update_persistent_subscription(
                    self.source_id.to_string().as_str(),
                    self.subscription_name,
                )
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

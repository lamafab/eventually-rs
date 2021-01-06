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
use uuid::Uuid;
use std::convert::TryFrom;
use std::fmt::Display;
use std::marker::PhantomData;
use std::pin::Pin;
use std::future::Future;

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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

        async fn y<Id>(mut stream: Pin<&mut PersistentStream<Id>>) -> Option<Result<Persisted<Id, GenericEvent>, StoreError>>
        where
            Id: TryFrom<String>,
        {
            // Scope 
            let event = {
                if let Some(event) = stream.as_mut().reader.try_next_event().await.unwrap() {
                    Some(event)
                } else {
                    None
                }
            };

            if let Some(event) = event {
                let processed = x::<Id>(event);

                if let Ok((_, uuid)) = processed {
                    stream.as_mut().writer.ack(vec![uuid]).await.unwrap();
                }

                Some(processed.map(|(persisted, _)| persisted))
            } else {
                None
            }
        }

        Box::pin(y(self)).as_mut().poll(cx)
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
            let (mut tx, rx) = mpsc::unbounded();

            // TODO: add trait bound for `AsRef<str>` instead of `Display`
            /*
            self.client
                .delete_persistent_subscription(
                    self.source_id.to_string().as_str(),
                    self.subscription_name,
                )
                .execute().await.unwrap();
            */

            self.client
                .create_persistent_subscription(
                    self.source_id.to_string().as_str(),
                    self.subscription_name,
                )
                .execute(PersistentSubscriptionSettings::default())
                .await?;

            self.client
                .connect_persistent_subscription(
                    self.source_id.to_string().as_str(),
                    self.subscription_name,
                )
                .execute()
                .await
                .map(|(mut read, mut write)| async move {
                    while let Some(sub_event) = read.try_next().await? {
                        println!("LOOP ENTRY");

                        let mut event = match sub_event {
                            SubEvent::EventAppeared(resolved) => resolved.event.unwrap(),
                            _ => continue,
                        };

                        println!("EVENT: {:?}", event);

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

                    println!("*** *** *** OUT");

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

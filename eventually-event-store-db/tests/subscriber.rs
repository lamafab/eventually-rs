mod common;

use common::{Event, SourceId};
use eventually::store::{EventStore, Expected};
use eventually::EventSubscriber;
use eventually_event_store_db::{EventStoreBuilder, GenericEvent};
use futures::stream::StreamExt;
use std::collections::HashSet;

#[tokio::test]
async fn event_store_db_subscribe_all() {
    let builder = EventStoreBuilder::new("esdb://localhost:2113?tls=false")
        .await
        .unwrap();

    let mut client = builder.build_store::<SourceId>();
    let subscriber = builder.clone().build_subscriber::<SourceId>();

    // Start from scratch.
    client.remove(SourceId::Baz).await.unwrap();
    client.remove(SourceId::Fum).await.unwrap();

    let handle = tokio::spawn(async move {
        // Expected events that should be picked up.
        let mut expected: HashSet<GenericEvent> =
            [Event::one(), Event::two(), Event::three(), Event::four()]
                .iter()
                .cloned()
                .collect();

        let mut stream = subscriber.subscribe_all().await.unwrap();
        while let Some(persisted) = stream.next().await {
            let raw_event = persisted.unwrap().take();

            expected.remove(&raw_event);

            if expected.is_empty() {
                break;
            }
        }

        assert!(expected.is_empty());
    });

    // Append the expected events.
    client
        .append(
            SourceId::Baz,
            Expected::Any,
            vec![Event::one(), Event::two()],
        )
        .await
        .unwrap();

    client
        .append(
            SourceId::Fum,
            Expected::Any,
            vec![Event::three(), Event::four()],
        )
        .await
        .unwrap();

    handle.await.unwrap();

    // Cleanup.
    client.remove(SourceId::Baz).await.unwrap();
    client.remove(SourceId::Fum).await.unwrap();
}
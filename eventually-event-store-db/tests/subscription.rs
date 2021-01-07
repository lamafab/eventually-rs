mod common;

use common::{Event, SourceId};
use eventually::store::{EventStore, Expected};
use eventually::versioning::Versioned;
use eventually::Subscription;
use eventually_event_store_db::{EventStoreBuilder, GenericEvent};
use futures::stream::StreamExt;
use std::collections::HashSet;

#[tokio::test]
async fn event_store_db_persistant_subscription() {
    let builder = EventStoreBuilder::new("esdb://localhost:2113?tls=false")
        .await
        .unwrap();

    let mut client = builder.build_store::<SourceId>();
    let subscription = builder
        .clone()
        .build_persistant_subscription::<SourceId>(SourceId::Grunt, "test_subscription");

    // Append the expected events.
    client
        .append(
            SourceId::Grunt,
            Expected::Any,
            vec![Event::one(), Event::two(), Event::three(), Event::four()],
        )
        .await
        .unwrap();

    // Read events from the beginning.
    let mut stream = subscription.resume().await.unwrap();

    let mut expected: HashSet<GenericEvent> =
        [Event::one(), Event::two()].iter().cloned().collect();

    while let Some(event) = stream.next().await {
        expected.remove(&event.unwrap().take());

        if expected.is_empty() {
            break;
        }
    }

    assert!(expected.is_empty());

    // Re-read events from the beginning (no checkpoint was set)
    let mut stream = subscription.resume().await.unwrap();

    let mut expected: HashSet<GenericEvent> =
        [Event::one(), Event::two()].iter().cloned().collect();

    // Get the ID of the last event.
    let mut at = None;
    while let Some(event) = stream.next().await {
        let event = event.unwrap();
        at = Some(event.version());

        expected.remove(&event.take());

        if expected.is_empty() {
            break;
        }
    }

    assert!(expected.is_empty());

    // Set checkpoint
    subscription.checkpoint(at.unwrap()).await.unwrap();

    // Read events after checkpoint
    let mut stream = subscription.resume().await.unwrap();

    let mut expected: HashSet<GenericEvent> =
        [Event::three(), Event::four()].iter().cloned().collect();

    while let Some(event) = stream.next().await {
        expected.remove(&event.unwrap().take());

        if expected.is_empty() {
            break;
        }
    }
}

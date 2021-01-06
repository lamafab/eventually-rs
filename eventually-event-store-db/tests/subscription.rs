mod common;

use common::{Event, SourceId};
use eventually::store::{EventStore, Expected};
use eventually::versioning::Versioned;
use eventually::Subscription;
use eventually_event_store_db::{EventStoreBuilder, GenericEvent};
use futures::{executor, stream::StreamExt};
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

    println!(">>> >>> SIX");
    let mut stream = subscription.resume().await.unwrap();

    let mut expected: HashSet<GenericEvent> =
        [Event::one(), Event::two()].iter().cloned().collect();

    println!(">>> >>> SEVEN");
    while let Some(event) = stream.next().await {
        expected.remove(&event.unwrap().take());

        if expected.is_empty() {
            break;
        }
    }

    assert!(expected.is_empty());

    println!(">>> >>> ONE");
    let mut stream = subscription.resume().await.unwrap();

    let mut expected: HashSet<GenericEvent> =
        [Event::one(), Event::two()].iter().cloned().collect();

    println!(">>> >>> TWO");
    let mut at = None;
    while let Some(event) = stream.next().await {
        let event = event.unwrap();
        at = Some(event.version());

        expected.remove(&event.take());

        println!("HIIIII");
        if expected.is_empty() {
            break;
        }
    }

    println!(">>> >>> THREE");
    assert!(expected.is_empty());

    subscription.checkpoint(at.unwrap()).await.unwrap();
}

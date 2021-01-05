mod common;

use common::{Event, SourceId};
use eventually_event_store_db::EventStoreBuilder;

#[tokio::test]
async fn event_store_db_verify_connection_valid() {
    let builder = EventStoreBuilder::new("esdb://localhost:2113?tls=false")
        .await
        .unwrap();

    let mut client = builder.build_store::<SourceId>();
    let subscription = builder
        .clone()
        .build_persistant_subscription::<SourceId>(SourceId::Grunt, "test_subscription");


}

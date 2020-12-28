//! EventStoreDB backend implementation for [`eventually` crate](https://crates.io/crates/eventually).

use eventstore::Client as EsClient;

mod store;
mod stream;
mod subscriber;
mod subscription;

/// Builder type for ['EventStore'].
pub struct EventStoreBuilder {
    client: EsClient,
}

impl EventStoreBuilder {
    /// Create a new builder instance from the connection string.
    pub async fn new(connection: &str) -> Self {
        EventStoreBuilder {
            client: EsClient::create(connection.parse().unwrap()).await.unwrap(),
        }
    }
}

impl From<EsClient> for EventStoreBuilder {
    fn from(client: EsClient) -> Self {
        EventStoreBuilder {
            client: client,
        }
    }
}

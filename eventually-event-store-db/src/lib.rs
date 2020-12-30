//! EventStoreDB backend implementation for [`eventually` crate](https://crates.io/crates/eventually).

// TODO: cast required?
use eventstore::{Client as EsClient, EventData};
use std::error::Error;

mod store;
mod stream;
mod subscriber;
mod subscription;

type Result<T> = std::result::Result<T, BuilderError>;

// Re-exports
pub use store::EventStore;

/// Error type returned by ['EventStoreBuilder'].
#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    /// Error returned when attempting to parse connection string.
    #[error("Failed to parse connection string")]
    InvalidConnection,
    /// Error returned when attempting to create a gRPC connection to the EventStoreDB databse.
    #[error("failed to create gRPC client to the EventStoreDB database")]
    GRpcClient(#[source] Box<dyn Error>),
    /// TODO
    #[error("connection verification timed-out. The connection string to the EventStoreDB might be wrong")]
    VerificationTimeout,
}

/// Builder type for ['EventStore'].
pub struct EventStoreBuilder {
    client: EsClient,
}

impl EventStoreBuilder {
    /// Create a new builder instance from the connection string.
    pub async fn new(connection: &str) -> Result<Self> {
        Ok(EventStoreBuilder {
            client: EsClient::create(
                connection
                    .parse()
                    .map_err(|_| BuilderError::InvalidConnection)?,
            )
            .await
            .map_err(|err| BuilderError::GRpcClient(err))?,
        })
    }
    /// TODO
    pub async fn verify_connection(&self, timeout: u64) -> Result<()> {
        EventStore::<(), ()>::verify_connection(&self.client, timeout)
            .await
            .map_err(|_| BuilderError::VerificationTimeout)
    }
    /// Builds the event store instance. This function can be called multiple times.
    pub fn build_store<Id, Event>(&self) -> EventStore<Id, Event> {
        EventStore::new(self.client.clone())
    }
}

impl From<EsClient> for EventStoreBuilder {
    fn from(client: EsClient) -> Self {
        EventStoreBuilder { client: client }
    }
}

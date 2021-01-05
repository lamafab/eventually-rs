//! EventStoreDB backend implementation for [`eventually` crate](https://crates.io/crates/eventually).

// TODO: cast required?
use bytes::Bytes;
use eventstore::Client as EsClient;
use serde::{Deserialize, Serialize};
use std::error::Error;

mod store;
mod subscriber;
mod subscription;

// TODO: Consider adjusting this
type Result<T> = std::result::Result<T, BuilderError>;

// Re-exports
pub use store::{EventStore, StoreError};
pub use subscriber::EventSubscriber;
pub use subscription::EventSubscription;

/// TODO
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct GenericEvent(Bytes);

impl GenericEvent {
    /// TODO
    pub fn serialize<T: Serialize>(val: T) -> std::result::Result<Self, serde_json::Error> {
        Ok(GenericEvent(serde_json::to_vec(&val)?.into()))
    }
    /// TODO
    pub fn as_json<'a, T: Deserialize<'a>>(&'a self) -> std::result::Result<T, serde_json::Error> {
        serde_json::from_slice(&self.0)
    }
    /// TODO
    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl From<Vec<u8>> for GenericEvent {
    fn from(val: Vec<u8>) -> Self {
        GenericEvent(val.into())
    }
}

impl From<Bytes> for GenericEvent {
    fn from(val: Bytes) -> Self {
        GenericEvent(val)
    }
}

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
#[derive(Clone)]
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
        EventStore::<()>::verify_connection(&self.client, timeout)
            .await
            .map_err(|_| BuilderError::VerificationTimeout)
    }
    /// Builds the event store instance. This function can be called multiple times.
    pub fn build_store<Id>(&self) -> EventStore<Id> {
        EventStore::new(self.client.clone())
    }
    /// TODO
    pub fn build_subscriber<Id>(&self) -> EventSubscriber<Id> {
        EventSubscriber::new(self.client.clone())
    }
    /// TODO
    pub fn build_persistant_subscription<Id>(
        &self,
        source_id: Id,
        subscription_name: &'static str,
    ) -> EventSubscription<Id> {
        EventSubscription::new(self.client.clone(), source_id, subscription_name)
    }
}

impl From<EsClient> for EventStoreBuilder {
    fn from(client: EsClient) -> Self {
        EventStoreBuilder { client: client }
    }
}

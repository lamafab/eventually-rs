//! EventStoreDB backend implementation for [`eventually` crate](https://crates.io/crates/eventually).

use eventstore::Client as EsClient;
use std::error::Error;

mod store;
mod stream;
mod subscriber;
mod subscription;

type Result<T> = std::result::Result<T, BuilderError>;

/// Error type returned by ['EventStoreBuilder'].
#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    /// Error returned when attempting to parse connection string.
    #[error("Failed to parse connection string")]
    InvalidConnection,
    /// Error returned when attempting to create a gRPC connection to the EventStoreDB databse.
    #[error("failed to create gRPC client to the EventStoreDB database")]
    GRpcClient(#[source] Box<dyn Error>),
}

/// Builder type for ['EventStore'].
pub struct EventStoreBuilder {
    client: EsClient,
}

impl EventStoreBuilder {
    /// Create a new builder instance from the connection string.
    pub async fn new(connection: &str) -> Result<Self> {
        Ok(
            EventStoreBuilder {
                client: EsClient::create(connection.parse().map_err(|_| BuilderError::InvalidConnection)?).await.map_err(|err| BuilderError::GRpcClient(err))?,
            }
        )
    }
}

impl From<EsClient> for EventStoreBuilder {
    fn from(client: EsClient) -> Self {
        EventStoreBuilder {
            client: client,
        }
    }
}

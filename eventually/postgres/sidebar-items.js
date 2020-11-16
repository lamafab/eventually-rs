initSidebarItems({"mod":[["store","Contains an [`EventStore`] implementation using PostgreSQL as a backend data store."],["subscriber","Contains an [`EventSubscriber`] implementation using PostgreSQL as a backend data store and `NOTIFY`/`LISTEN` functionality to power the [`EventStream`]."],["subscription","Contains a persisted implementation of the [`Subscription`] trait using Postgres as the backend data source for its state."]],"struct":[["DeserializeError","Error returned by the `TryStream` on [`subscribe_all`] when deserializing payloads coming from Postgres' `LISTEN` asynchronous notifications."],["EventStore","[`EventStore`] implementation using a PostgreSQL backend."],["EventStoreBuilder","Builder type for [`EventStore`] instances."],["EventStoreBuilderMigrated","Builder step for [`EventStore`] instances, after the database migration executed from [`EventStoreBuilder`] has been completed."],["EventSubscriber","Subscriber for listening to new events committed to an [`EventStore`], using Postgres' `LISTEN` functionality."],["Persistent","[`Subscription`] type with persistent state over a Postgres data source."],["PersistentBuilder","Builder type for multiple [`Persistent`] Subscription instance."]],"type":[["PoolResult","Result returning the connection pool [`Error`] type."]]});
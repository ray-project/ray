//! Storage layer for Ray GCS.
//!
//! This crate provides the `StoreClient` trait (matching the C++ `StoreClient` interface)
//! and its implementations: `InMemoryStoreClient` and `RedisStoreClient`.

mod store_client;
mod in_memory_store_client;
mod redis_store_client;

pub use store_client::StoreClient;
pub use in_memory_store_client::InMemoryStoreClient;
pub use redis_store_client::RedisStoreClient;

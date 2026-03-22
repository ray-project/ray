// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Pub/sub messaging system for Ray.
//!
//! Replaces `src/ray/pubsub/` (14 files).
//! Implements the GCS-backed publish/subscribe pattern using long-polling.
//!
//! The publisher manages per-channel subscription indices, per-subscriber
//! message mailboxes with configurable buffer limits, and dead subscriber
//! detection.
//!
//! The subscriber manages long-poll connections to publishers, command
//! batching, sequence ID tracking for deduplication, and publisher
//! failover detection.

pub mod publisher;
pub mod subscriber;
pub mod transport;

pub use publisher::{PubMessage, Publisher, PublisherConfig};
pub use subscriber::{
    FailureCallback, MessageCallback, Subscriber, SubscriberCommand, SubscriberConfig,
};
pub use transport::{
    FailingSubscriberClient, InProcessSubscriberClient, SubscriberClient, SubscriberTransport,
    TransportError,
};

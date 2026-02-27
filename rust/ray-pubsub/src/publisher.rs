// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Publisher side of Ray pub/sub.

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

/// A pub/sub channel identifier.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ChannelId(pub String);

/// Publisher that manages broadcast channels for different topics.
pub struct Publisher {
    channels: Arc<DashMap<ChannelId, broadcast::Sender<Vec<u8>>>>,
    channel_capacity: usize,
}

impl Publisher {
    pub fn new(channel_capacity: usize) -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
            channel_capacity,
        }
    }

    /// Publish a message to a channel.
    pub fn publish(&self, channel_id: &ChannelId, message: Vec<u8>) -> bool {
        if let Some(sender) = self.channels.get(channel_id) {
            sender.send(message).is_ok()
        } else {
            false
        }
    }

    /// Get or create a broadcast channel and return a receiver.
    pub fn subscribe(&self, channel_id: ChannelId) -> broadcast::Receiver<Vec<u8>> {
        let entry = self
            .channels
            .entry(channel_id)
            .or_insert_with(|| broadcast::channel(self.channel_capacity).0);
        entry.subscribe()
    }

    /// Remove a channel.
    pub fn unsubscribe(&self, channel_id: &ChannelId) {
        self.channels.remove(channel_id);
    }

    /// Number of active channels.
    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }
}

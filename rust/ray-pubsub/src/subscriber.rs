// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Subscriber side of Ray pub/sub with long-poll support.

use tokio::sync::broadcast;

/// A subscriber that receives messages from a broadcast channel.
pub struct Subscriber {
    receiver: broadcast::Receiver<Vec<u8>>,
}

impl Subscriber {
    pub fn new(receiver: broadcast::Receiver<Vec<u8>>) -> Self {
        Self { receiver }
    }

    /// Wait for the next message (long-poll pattern).
    pub async fn recv(&mut self) -> Result<Vec<u8>, SubscribeError> {
        self.receiver.recv().await.map_err(|e| match e {
            broadcast::error::RecvError::Closed => SubscribeError::ChannelClosed,
            broadcast::error::RecvError::Lagged(n) => SubscribeError::Lagged(n),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("channel closed")]
    ChannelClosed,
    #[error("subscriber lagged by {0} messages")]
    Lagged(u64),
}

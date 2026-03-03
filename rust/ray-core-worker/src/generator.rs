// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Generator / streaming task support.
//!
//! Implements flow control for tasks that yield multiple intermediate
//! results (e.g., Ray Data streaming, generator functions). The
//! `GeneratorBackpressureWaiter` blocks the producer until the consumer
//! has consumed previous results, preventing unbounded memory growth.
//!
//! Replaces `src/ray/core_worker/generator_waiter.cc`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use ray_common::id::ObjectID;

/// Configuration for generator back-pressure.
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Maximum number of unconsumed intermediate results before blocking.
    pub max_buffered_results: usize,
    /// Maximum total bytes of unconsumed results before blocking.
    pub max_buffered_bytes: usize,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            max_buffered_results: 100,
            max_buffered_bytes: 100 * 1024 * 1024, // 100MB
        }
    }
}

/// An intermediate result yielded by a generator task.
#[derive(Debug, Clone)]
pub struct IntermediateResult {
    /// The ObjectID for this intermediate result.
    pub object_id: ObjectID,
    /// Size of the result data in bytes.
    pub data_size: usize,
    /// Sequence number within the generator stream.
    pub sequence_number: u64,
}

/// Flow control for generator/streaming tasks.
///
/// The producer (generator task) calls `yield_result()` to buffer an
/// intermediate result. If the buffer is full, it blocks until the
/// consumer calls `consume_result()`.
pub struct GeneratorBackpressureWaiter {
    config: GeneratorConfig,
    /// Buffered intermediate results.
    buffer: Mutex<VecDeque<IntermediateResult>>,
    /// Total bytes buffered.
    buffered_bytes: AtomicUsize,
    /// Sequence counter for ordering.
    next_sequence: AtomicUsize,
    /// Notification for waking blocked producers.
    space_available: Arc<Notify>,
    /// Notification for waking consumers.
    result_available: Arc<Notify>,
    /// Whether the generator has finished.
    is_finished: AtomicBool,
    /// Total results yielded.
    total_yielded: AtomicUsize,
    /// Total results consumed.
    total_consumed: AtomicUsize,
}

impl GeneratorBackpressureWaiter {
    pub fn new(config: GeneratorConfig) -> Self {
        Self {
            config,
            buffer: Mutex::new(VecDeque::new()),
            buffered_bytes: AtomicUsize::new(0),
            next_sequence: AtomicUsize::new(0),
            space_available: Arc::new(Notify::new()),
            result_available: Arc::new(Notify::new()),
            is_finished: AtomicBool::new(false),
            total_yielded: AtomicUsize::new(0),
            total_consumed: AtomicUsize::new(0),
        }
    }

    /// Yield an intermediate result from the generator.
    ///
    /// Blocks if the buffer is full until the consumer consumes results.
    /// Returns the sequence number assigned to this result.
    pub async fn yield_result(&self, object_id: ObjectID, data_size: usize) -> u64 {
        // Wait until there's space in the buffer.
        loop {
            let buffered = self.buffer.lock().len();
            let bytes = self.buffered_bytes.load(Ordering::Relaxed);

            if buffered < self.config.max_buffered_results
                && bytes + data_size <= self.config.max_buffered_bytes
            {
                break;
            }

            // Buffer full — wait for consumer.
            self.space_available.notified().await;
        }

        let seq = self.next_sequence.fetch_add(1, Ordering::Relaxed) as u64;
        let result = IntermediateResult {
            object_id,
            data_size,
            sequence_number: seq,
        };

        self.buffer.lock().push_back(result);
        self.buffered_bytes.fetch_add(data_size, Ordering::Relaxed);
        self.total_yielded.fetch_add(1, Ordering::Relaxed);

        // Notify consumer.
        self.result_available.notify_one();

        seq
    }

    /// Consume the next intermediate result.
    ///
    /// Returns `None` if the generator has finished and the buffer is empty.
    /// Blocks if the buffer is empty and the generator is still running.
    pub async fn consume_result(&self) -> Option<IntermediateResult> {
        loop {
            // Try to pop from buffer.
            if let Some(result) = self.buffer.lock().pop_front() {
                self.buffered_bytes
                    .fetch_sub(result.data_size, Ordering::Relaxed);
                self.total_consumed.fetch_add(1, Ordering::Relaxed);

                // Notify producer that space is available.
                self.space_available.notify_one();
                return Some(result);
            }

            // Buffer empty.
            if self.is_finished.load(Ordering::Relaxed) {
                return None;
            }

            // Wait for producer.
            self.result_available.notified().await;
        }
    }

    /// Try to consume a result without blocking.
    pub fn try_consume(&self) -> Option<IntermediateResult> {
        let result = self.buffer.lock().pop_front()?;
        self.buffered_bytes
            .fetch_sub(result.data_size, Ordering::Relaxed);
        self.total_consumed.fetch_add(1, Ordering::Relaxed);
        self.space_available.notify_one();
        Some(result)
    }

    /// Mark the generator as finished (no more results will be yielded).
    pub fn finish(&self) {
        self.is_finished.store(true, Ordering::Relaxed);
        self.result_available.notify_waiters();
    }

    /// Whether the generator has finished.
    pub fn is_finished(&self) -> bool {
        self.is_finished.load(Ordering::Relaxed)
    }

    /// Number of results currently buffered.
    pub fn num_buffered(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Total bytes currently buffered.
    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes.load(Ordering::Relaxed)
    }

    /// Total results yielded by the generator.
    pub fn total_yielded(&self) -> usize {
        self.total_yielded.load(Ordering::Relaxed)
    }

    /// Total results consumed.
    pub fn total_consumed(&self) -> usize {
        self.total_consumed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_oid(val: u8) -> ObjectID {
        let mut data = [0u8; 28];
        data[0] = val;
        ObjectID::from_binary(&data)
    }

    #[tokio::test]
    async fn test_yield_and_consume() {
        let waiter = GeneratorBackpressureWaiter::new(GeneratorConfig::default());

        let seq = waiter.yield_result(make_oid(1), 100).await;
        assert_eq!(seq, 0);
        assert_eq!(waiter.num_buffered(), 1);
        assert_eq!(waiter.buffered_bytes(), 100);

        let result = waiter.consume_result().await.unwrap();
        assert_eq!(result.object_id, make_oid(1));
        assert_eq!(result.data_size, 100);
        assert_eq!(result.sequence_number, 0);
        assert_eq!(waiter.num_buffered(), 0);
        assert_eq!(waiter.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn test_multiple_yield_consume() {
        let waiter = GeneratorBackpressureWaiter::new(GeneratorConfig::default());

        for i in 0..5u8 {
            waiter.yield_result(make_oid(i), 10).await;
        }
        assert_eq!(waiter.num_buffered(), 5);
        assert_eq!(waiter.total_yielded(), 5);

        for i in 0..5u8 {
            let result = waiter.consume_result().await.unwrap();
            assert_eq!(result.object_id, make_oid(i));
            assert_eq!(result.sequence_number, i as u64);
        }
        assert_eq!(waiter.total_consumed(), 5);
    }

    #[tokio::test]
    async fn test_finish_signals_consumer() {
        let waiter = Arc::new(GeneratorBackpressureWaiter::new(GeneratorConfig::default()));

        let waiter2 = waiter.clone();
        let handle = tokio::spawn(async move {
            let result = waiter2.consume_result().await;
            result
        });

        // Give consumer time to block.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        waiter.finish();
        let result = handle.await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_consume() {
        let waiter = GeneratorBackpressureWaiter::new(GeneratorConfig::default());

        assert!(waiter.try_consume().is_none());

        waiter.yield_result(make_oid(1), 100).await;
        let result = waiter.try_consume().unwrap();
        assert_eq!(result.object_id, make_oid(1));

        assert!(waiter.try_consume().is_none());
    }

    #[tokio::test]
    async fn test_backpressure() {
        let waiter = Arc::new(GeneratorBackpressureWaiter::new(GeneratorConfig {
            max_buffered_results: 2,
            max_buffered_bytes: usize::MAX,
        }));

        // Fill the buffer.
        waiter.yield_result(make_oid(1), 10).await;
        waiter.yield_result(make_oid(2), 10).await;
        assert_eq!(waiter.num_buffered(), 2);

        // Third yield should block.
        let waiter2 = waiter.clone();
        let handle = tokio::spawn(async move {
            waiter2.yield_result(make_oid(3), 10).await
        });

        // Give producer time to start waiting.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(waiter.num_buffered(), 2); // Still 2, third is blocked.

        // Consume one to unblock.
        let _ = waiter.consume_result().await;
        let seq = handle.await.unwrap();
        assert_eq!(seq, 2);
        assert_eq!(waiter.num_buffered(), 2); // 2 remaining (oid2, oid3)
    }

    #[tokio::test]
    async fn test_bytes_backpressure() {
        let waiter = Arc::new(GeneratorBackpressureWaiter::new(GeneratorConfig {
            max_buffered_results: usize::MAX,
            max_buffered_bytes: 200,
        }));

        waiter.yield_result(make_oid(1), 150).await;
        assert_eq!(waiter.buffered_bytes(), 150);

        // Next yield of 100 bytes would exceed 200 limit.
        let waiter2 = waiter.clone();
        let handle = tokio::spawn(async move {
            waiter2.yield_result(make_oid(2), 100).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(waiter.num_buffered(), 1); // Second is blocked.

        // Consume to free space.
        let _ = waiter.consume_result().await;
        handle.await.unwrap();
        assert_eq!(waiter.num_buffered(), 1);
    }

    #[tokio::test]
    async fn test_sequence_numbers() {
        let waiter = GeneratorBackpressureWaiter::new(GeneratorConfig::default());

        let s0 = waiter.yield_result(make_oid(1), 10).await;
        let s1 = waiter.yield_result(make_oid(2), 10).await;
        let s2 = waiter.yield_result(make_oid(3), 10).await;

        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
    }

    #[test]
    fn test_default_config() {
        let config = GeneratorConfig::default();
        assert_eq!(config.max_buffered_results, 100);
        assert_eq!(config.max_buffered_bytes, 100 * 1024 * 1024);
    }
}

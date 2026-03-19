#![allow(clippy::needless_update)]
// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Concurrent stress tests for the Rust GCS InternalPubSubHandler.

use std::sync::Arc;

use ray_gcs::pubsub_handler::InternalPubSubHandler;
use tokio::time::{timeout, Duration};

/// Helper to construct a PubMessage for a given channel type and key.
fn make_pub_msg(channel_type: i32, key_id: &[u8]) -> ray_proto::ray::rpc::PubMessage {
    ray_proto::ray::rpc::PubMessage {
        channel_type,
        key_id: key_id.to_vec(),
        ..Default::default()
    }
}

/// 100 concurrent tokio tasks each publish 100 messages to the same pubsub handler.
/// A single subscriber should receive all 10,000 messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_pubsub_publish() {
    let result = timeout(Duration::from_secs(5), async {
        let handler = Arc::new(InternalPubSubHandler::new());

        // Register a single subscriber interested in all keys on channel 3.
        handler.handle_subscribe_command(b"sub1".to_vec(), 3, vec![]);

        let num_tasks = 100;
        let msgs_per_task = 100;

        let mut handles = Vec::with_capacity(num_tasks);
        for task_id in 0..num_tasks {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                for msg_id in 0..msgs_per_task {
                    let key = format!("task{}-msg{}", task_id, msg_id);
                    h.publish_pubmessage(make_pub_msg(3, key.as_bytes()));
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Collect all pending messages via poll. Since all publishes are done,
        // pending_messages should contain all 10,000 messages.
        let messages = handler.handle_subscriber_poll(b"sub1", 0).await;
        assert_eq!(
            messages.len(),
            num_tasks * msgs_per_task,
            "expected {} messages but got {}",
            num_tasks * msgs_per_task,
            messages.len()
        );
    })
    .await;

    assert!(result.is_ok(), "test_concurrent_pubsub_publish timed out (deadlock?)");
}

/// 50 tasks subscribing and 50 tasks unsubscribing concurrently.
/// Should not panic or deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_subscribe_unsubscribe() {
    let result = timeout(Duration::from_secs(5), async {
        let handler = Arc::new(InternalPubSubHandler::new());

        let mut handles = Vec::with_capacity(100);

        // 50 subscribe tasks
        for i in 0..50 {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                let sub_id = format!("sub-{}", i);
                h.handle_subscribe_command(sub_id.into_bytes(), 3, vec![]);
            }));
        }

        // 50 unsubscribe tasks (targeting the same subscriber IDs)
        for i in 0..50 {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                let sub_id = format!("sub-{}", i);
                h.handle_unsubscribe_command(sub_id.as_bytes(), 3);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // If we got here without panic or deadlock, the test passes.
    })
    .await;

    assert!(
        result.is_ok(),
        "test_concurrent_subscribe_unsubscribe timed out (deadlock?)"
    );
}

/// 20 publishers sending messages while 20 subscribers poll concurrently.
/// All polls should complete within the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_publish_and_poll() {
    let result = timeout(Duration::from_secs(5), async {
        let handler = Arc::new(InternalPubSubHandler::new());

        // Register 20 subscribers
        for i in 0..20 {
            let sub_id = format!("poller-{}", i);
            handler.handle_subscribe_command(sub_id.into_bytes(), 3, vec![]);
        }

        let mut handles = Vec::with_capacity(40);

        // 20 publisher tasks
        for task_id in 0..20 {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                for msg_id in 0..50 {
                    let key = format!("pub{}-msg{}", task_id, msg_id);
                    h.publish_pubmessage(make_pub_msg(3, key.as_bytes()));
                    // Yield occasionally to interleave with pollers.
                    if msg_id % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        // 20 poller tasks — each polls multiple times
        for i in 0..20 {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                let sub_id = format!("poller-{}", i);
                let mut max_seq: i64 = 0;
                for _ in 0..5 {
                    let poll_result = timeout(
                        Duration::from_secs(2),
                        h.handle_subscriber_poll(sub_id.as_bytes(), max_seq),
                    )
                    .await;
                    if let Ok(messages) = poll_result {
                        if let Some(seq) = messages.iter().map(|m| m.sequence_id).max() {
                            max_seq = seq;
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "test_concurrent_publish_and_poll timed out (deadlock?)"
    );
}

/// 200 tasks all publishing to the same channel type simultaneously.
/// Verify sequence IDs are monotonically increasing per subscriber.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_high_contention_publish() {
    let result = timeout(Duration::from_secs(5), async {
        let handler = Arc::new(InternalPubSubHandler::new());

        handler.handle_subscribe_command(b"observer".to_vec(), 3, vec![]);

        let num_tasks = 200;
        let mut handles = Vec::with_capacity(num_tasks);

        for task_id in 0..num_tasks {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                let key = format!("high-contention-{}", task_id);
                h.publish_pubmessage(make_pub_msg(3, key.as_bytes()));
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Drain all messages and verify monotonicity.
        let messages = handler.handle_subscriber_poll(b"observer", 0).await;
        assert_eq!(messages.len(), num_tasks);

        let seq_ids: Vec<i64> = messages.iter().map(|m| m.sequence_id).collect();
        for i in 1..seq_ids.len() {
            assert!(
                seq_ids[i] > seq_ids[i - 1],
                "sequence IDs must be monotonically increasing, but seq[{}]={} <= seq[{}]={}",
                i,
                seq_ids[i],
                i - 1,
                seq_ids[i - 1]
            );
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "test_high_contention_publish timed out (deadlock?)"
    );
}

/// Rapidly subscribe/unsubscribe while publishing. No panics, no deadlocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_pubsub_under_subscribe_churn() {
    let result = timeout(Duration::from_secs(5), async {
        let handler = Arc::new(InternalPubSubHandler::new());

        let mut handles = Vec::with_capacity(150);

        // 50 tasks that rapidly subscribe and unsubscribe
        for i in 0..50 {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                for round in 0..20 {
                    let sub_id = format!("churn-{}-{}", i, round);
                    h.handle_subscribe_command(sub_id.clone().into_bytes(), 3, vec![]);
                    // Yield to let publishers interleave.
                    tokio::task::yield_now().await;
                    h.handle_unsubscribe_command(sub_id.as_bytes(), 3);
                }
            }));
        }

        // 50 tasks that publish during the churn
        for task_id in 0..50 {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                for msg_id in 0..20 {
                    let key = format!("churn-pub-{}-{}", task_id, msg_id);
                    h.publish_pubmessage(make_pub_msg(3, key.as_bytes()));
                    tokio::task::yield_now().await;
                }
            }));
        }

        // 50 tasks that subscribe, poll once, then unsubscribe
        for i in 0..50 {
            let h = Arc::clone(&handler);
            handles.push(tokio::spawn(async move {
                let sub_id = format!("churn-poll-{}", i);
                h.handle_subscribe_command(sub_id.clone().into_bytes(), 3, vec![]);
                // Short poll with timeout — we don't care about the result,
                // only that it doesn't panic or deadlock.
                let _ = timeout(
                    Duration::from_millis(100),
                    h.handle_subscriber_poll(sub_id.as_bytes(), 0),
                )
                .await;
                h.handle_unsubscribe_command(sub_id.as_bytes(), 3);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // If we reach here, no panics or deadlocks occurred.
    })
    .await;

    assert!(
        result.is_ok(),
        "test_pubsub_under_subscribe_churn timed out (deadlock?)"
    );
}

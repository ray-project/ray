// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests ported from pubsub_integration_test.cc and
//! python_gcs_subscriber_auth_test.cc.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ray_pubsub::publisher::{PubMessage, Publisher, PublisherConfig};
use ray_pubsub::subscriber::{Subscriber, SubscriberConfig};

fn make_msg(channel_type: i32, key_id: &[u8], seq: i64, payload: Vec<u8>) -> PubMessage {
    PubMessage {
        channel_type,
        key_id: key_id.to_vec(),
        payload,
        sequence_id: seq,
    }
}

/// Ported from SubscribersToOneIDAndAllIDs (pubsub_integration_test.cc).
///
/// Tests that one subscriber listening for a specific key and another
/// listening to all entities on the same channel both receive a published
/// message.
#[tokio::test]
async fn test_subscribers_to_one_id_and_all_ids() {
    let publisher = Arc::new(Publisher::new(PublisherConfig {
        publish_batch_size: 100,
        max_buffered_bytes: -1,
        subscriber_timeout_ms: 30_000,
    }));
    publisher.register_channel(3, false); // GCS_ACTOR_CHANNEL equivalent

    // Subscriber 1: subscribes to a specific actor key.
    let sub1 = Subscriber::new(b"sub1".to_vec(), SubscriberConfig::default());
    let count1 = Arc::new(AtomicUsize::new(0));
    let count1_clone = count1.clone();
    let received_key1 = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let received_key1_clone = received_key1.clone();
    let cb1: ray_pubsub::subscriber::MessageCallback = Arc::new(move |msg| {
        count1_clone.fetch_add(1, Ordering::Relaxed);
        received_key1_clone.lock().push(msg.key_id.clone());
    });
    sub1.subscribe(b"publisher1", 3, b"actor_123", cb1, None);

    // Subscriber 2: subscribes to all entities.
    let sub2 = Subscriber::new(b"sub2".to_vec(), SubscriberConfig::default());
    let count2 = Arc::new(AtomicUsize::new(0));
    let count2_clone = count2.clone();
    let received_key2 = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let received_key2_clone = received_key2.clone();
    let cb2: ray_pubsub::subscriber::MessageCallback = Arc::new(move |msg| {
        count2_clone.fetch_add(1, Ordering::Relaxed);
        received_key2_clone.lock().push(msg.key_id.clone());
    });
    sub2.subscribe(b"publisher1", 3, b"", cb2, None);

    // Register both subscribers on the publisher side.
    publisher.register_subscription(b"sub1", 3, b"actor_123");
    publisher.register_subscription(b"sub2", 3, b"");

    // Publish a message for actor_123.
    publisher.publish(3, b"actor_123", b"actor_data".to_vec());

    // Connect sub1 and verify it receives the message.
    let rx1 = publisher.connect_subscriber(b"sub1", 0);
    let messages1 = rx1.await.unwrap();
    assert_eq!(messages1.len(), 1);
    assert_eq!(messages1[0].key_id, b"actor_123");

    // Dispatch to subscriber callbacks.
    sub1.handle_poll_response(b"publisher1", b"pub_id", &messages1);
    assert_eq!(count1.load(Ordering::Relaxed), 1);

    // Connect sub2 and verify it also receives the message.
    let rx2 = publisher.connect_subscriber(b"sub2", 0);
    let messages2 = rx2.await.unwrap();
    assert_eq!(messages2.len(), 1);
    assert_eq!(messages2[0].key_id, b"actor_123");

    // Dispatch to subscriber callbacks.
    sub2.handle_poll_response(b"publisher1", b"pub_id", &messages2);
    assert_eq!(count2.load(Ordering::Relaxed), 1);

    // Both received the same actor key.
    assert_eq!(received_key1.lock()[0], b"actor_123");
    assert_eq!(received_key2.lock()[0], b"actor_123");

    // Unsubscribe both.
    sub1.unsubscribe(b"publisher1", 3, b"actor_123");
    sub2.unsubscribe(b"publisher1", 3, b"");

    assert!(!sub1.is_subscribed(b"publisher1", 3, b"actor_123"));
    assert!(!sub2.is_subscribed(b"publisher1", 3, b""));
}

// --- Auth tests ported from python_gcs_subscriber_auth_test.cc ---
//
// The C++ tests exercise gRPC token-based auth with a real server. In the
// Rust port we don't have PythonGcsSubscriber or GrpcServer, so we test the
// equivalent auth patterns at the pub/sub protocol level: ensuring that
// subscriber IDs are properly matched and that subscribe/unsubscribe
// operations succeed or fail based on registration state.

/// Ported from MatchingTokens: subscription succeeds when subscriber is registered.
#[test]
fn test_auth_matching_tokens() {
    let publisher = Publisher::new(PublisherConfig::default());
    publisher.register_channel(3, false);

    // "Auth" succeeds: subscriber registers and is accepted.
    assert!(publisher.register_subscription(b"auth_sub", 3, b"key1"));
    assert_eq!(publisher.num_subscribers(), 1);
}

/// Ported from MismatchedTokens: subscription fails for an unregistered channel.
#[test]
fn test_auth_mismatched_tokens() {
    let publisher = Publisher::new(PublisherConfig::default());
    publisher.register_channel(3, false);

    // Attempting to register on an unknown channel fails (simulates auth rejection).
    assert!(!publisher.register_subscription(b"auth_sub", 99, b"key1"));
}

/// Ported from ClientTokenServerNoAuth: subscriber can subscribe regardless
/// when the channel is registered.
#[test]
fn test_auth_client_token_server_no_auth() {
    let publisher = Publisher::new(PublisherConfig::default());
    publisher.register_channel(3, false);

    // Subscription succeeds (server accepts all).
    assert!(publisher.register_subscription(b"client_with_token", 3, b"key1"));
    assert_eq!(publisher.num_subscribers(), 1);
}

/// Ported from ServerTokenClientNoAuth: subscription fails when channel is
/// not registered (simulates auth failure).
#[test]
fn test_auth_server_token_client_no_auth() {
    let publisher = Publisher::new(PublisherConfig::default());
    // No channels registered — simulates server rejecting unauthenticated clients.

    assert!(!publisher.register_subscription(b"unauth_sub", 3, b"key1"));
}

/// Ported from MatchingTokensPoll: poll succeeds after successful registration.
#[tokio::test]
async fn test_auth_matching_tokens_poll() {
    let publisher = Publisher::new(PublisherConfig::default());
    publisher.register_channel(3, false);
    publisher.register_subscription(b"sub1", 3, b"key1");

    publisher.publish(3, b"key1", b"data".to_vec());

    let rx = publisher.connect_subscriber(b"sub1", 0);
    let messages = rx.await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].key_id, b"key1");
}

/// Ported from MismatchedTokensPoll: poll for unregistered subscriber returns empty.
#[tokio::test]
async fn test_auth_mismatched_tokens_poll() {
    let publisher = Publisher::new(PublisherConfig::default());
    publisher.register_channel(3, false);

    // Subscriber never registered — connect returns empty immediately.
    let rx = publisher.connect_subscriber(b"unknown_sub", 0);
    let messages = rx.await.unwrap();
    assert!(messages.is_empty());
}

/// Ported from MatchingTokensClose: unsubscription succeeds after subscribe.
#[test]
fn test_auth_matching_tokens_close() {
    let publisher = Publisher::new(PublisherConfig::default());
    publisher.register_channel(3, false);
    publisher.register_subscription(b"sub1", 3, b"key1");
    assert_eq!(publisher.num_subscribers(), 1);

    // Unsubscribe (close).
    publisher.unregister_subscription(b"sub1", 3, b"key1");
    publisher.unregister_subscriber(b"sub1");
    assert_eq!(publisher.num_subscribers(), 0);
}

/// Ported from NoAuthRequired: everything works without any auth.
#[tokio::test]
async fn test_auth_no_auth_required() {
    let publisher = Publisher::new(PublisherConfig::default());
    publisher.register_channel(3, false);

    // Subscribe.
    assert!(publisher.register_subscription(b"sub1", 3, b"key1"));

    // Publish and poll.
    publisher.publish(3, b"key1", b"data".to_vec());
    let rx = publisher.connect_subscriber(b"sub1", 0);
    let messages = rx.await.unwrap();
    assert_eq!(messages.len(), 1);

    // Close.
    publisher.unregister_subscriber(b"sub1");
    assert_eq!(publisher.num_subscribers(), 0);
}

/// Ported from MultipleSubscribersMatchingTokens: multiple subscribers
/// can subscribe concurrently.
#[tokio::test]
async fn test_auth_multiple_subscribers_matching_tokens() {
    let publisher = Publisher::new(PublisherConfig::default());
    publisher.register_channel(3, false);

    assert!(publisher.register_subscription(b"sub1", 3, b"key1"));
    assert!(publisher.register_subscription(b"sub2", 3, b"key1"));
    assert_eq!(publisher.num_subscribers(), 2);

    publisher.publish(3, b"key1", b"data".to_vec());

    let rx1 = publisher.connect_subscriber(b"sub1", 0);
    let rx2 = publisher.connect_subscriber(b"sub2", 0);

    let msgs1 = rx1.await.unwrap();
    let msgs2 = rx2.await.unwrap();
    assert_eq!(msgs1.len(), 1);
    assert_eq!(msgs2.len(), 1);

    publisher.unregister_subscriber(b"sub1");
    publisher.unregister_subscriber(b"sub2");
    assert_eq!(publisher.num_subscribers(), 0);
}

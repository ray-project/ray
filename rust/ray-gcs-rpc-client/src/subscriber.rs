// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS subscriber client — long-poll-based subscriber to GCS pubsub.
//!
//! Subscribes to GCS channels and invokes callbacks when messages arrive.
//! Uses the `InternalPubSubGcsService` long-poll RPCs:
//! - `GcsSubscriberCommandBatch` to register subscriptions
//! - `GcsSubscriberPoll` to receive published messages

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tonic::transport::Channel;
use tonic::Status;

use ray_proto::ray::rpc;

type PubSubClient =
    rpc::internal_pub_sub_gcs_service_client::InternalPubSubGcsServiceClient<Channel>;

/// Callback invoked when a PubMessage is received from GCS.
pub type SubscriberCallback = Box<dyn Fn(rpc::PubMessage) + Send + Sync>;

/// Per-channel callback list: Vec of (key_filter, callback).
type ChannelCallbackList = Vec<(Vec<u8>, Arc<SubscriberCallback>)>;

/// A subscription entry: channel + optional key filter.
struct Subscription {
    channel_type: i32,
    key_id: Vec<u8>,
}

/// Client-side GCS subscriber that long-polls for published messages.
pub struct GcsSubscriberClient {
    /// Unique subscriber ID (typically the worker/component ID).
    subscriber_id: Vec<u8>,
    /// Registered subscriptions.
    subscriptions: Mutex<Vec<Subscription>>,
    /// Callbacks indexed by channel_type for fast dispatch.
    channel_callbacks: Mutex<HashMap<i32, ChannelCallbackList>>,
    /// The max sequence ID we have processed.
    max_processed_sequence_id: Mutex<i64>,
    /// Notification to stop the polling loop.
    shutdown: Notify,
    /// Whether the polling loop is running.
    is_running: Mutex<bool>,
    /// Poll timeout for long-poll requests.
    poll_timeout: Duration,
}

impl GcsSubscriberClient {
    /// Create a new subscriber client with the given subscriber ID.
    pub fn new(subscriber_id: Vec<u8>) -> Self {
        Self {
            subscriber_id,
            subscriptions: Mutex::new(Vec::new()),
            channel_callbacks: Mutex::new(HashMap::new()),
            max_processed_sequence_id: Mutex::new(0),
            shutdown: Notify::new(),
            is_running: Mutex::new(false),
            poll_timeout: Duration::from_secs(30),
        }
    }

    /// Create with a custom poll timeout.
    pub fn with_poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Subscribe to a channel. The callback will be invoked for each matching message.
    ///
    /// - `channel_type`: The pubsub channel (e.g., GcsActorChannel = 3)
    /// - `key_id`: Specific key to filter on, or empty for all keys
    /// - `callback`: Invoked for each received PubMessage
    pub fn subscribe(
        &self,
        channel_type: i32,
        key_id: Vec<u8>,
        callback: SubscriberCallback,
    ) {
        let cb = Arc::new(callback);
        self.subscriptions.lock().push(Subscription {
            channel_type,
            key_id: key_id.clone(),
        });
        self.channel_callbacks
            .lock()
            .entry(channel_type)
            .or_default()
            .push((key_id, cb));
    }

    /// Build the command batch request to register all subscriptions with GCS.
    fn build_command_batch(&self) -> rpc::GcsSubscriberCommandBatchRequest {
        let subs = self.subscriptions.lock();
        let commands: Vec<rpc::Command> = subs
            .iter()
            .map(|s| rpc::Command {
                channel_type: s.channel_type,
                key_id: s.key_id.clone(),
                command_message_one_of: Some(
                    rpc::command::CommandMessageOneOf::SubscribeMessage(
                        rpc::SubMessage {
                            sub_message_one_of: None,
                        },
                    ),
                ),
            })
            .collect();

        rpc::GcsSubscriberCommandBatchRequest {
            subscriber_id: self.subscriber_id.clone(),
            commands,
            sender_id: self.subscriber_id.clone(),
        }
    }

    /// Dispatch received messages to registered callbacks.
    fn dispatch_messages(&self, messages: Vec<rpc::PubMessage>) {
        let callbacks = self.channel_callbacks.lock();
        let mut max_seq = *self.max_processed_sequence_id.lock();

        for msg in messages {
            if msg.sequence_id > max_seq {
                max_seq = msg.sequence_id;
            }
            if let Some(cbs) = callbacks.get(&msg.channel_type) {
                for (key_filter, cb) in cbs {
                    // Empty key_filter means all keys; otherwise match specific key
                    if key_filter.is_empty() || *key_filter == msg.key_id {
                        cb(msg.clone());
                    }
                }
            }
        }

        *self.max_processed_sequence_id.lock() = max_seq;
    }

    /// Start the background polling loop. Registers subscriptions then
    /// continuously long-polls for messages.
    ///
    /// Returns a JoinHandle for the background task.
    pub fn start(
        self: &Arc<Self>,
        mut stub: PubSubClient,
    ) -> tokio::task::JoinHandle<Result<(), Status>> {
        let this = Arc::clone(self);
        *this.is_running.lock() = true;

        tokio::spawn(async move {
            // Step 1: Register subscriptions
            let cmd_batch = this.build_command_batch();
            stub.gcs_subscriber_command_batch(tonic::Request::new(cmd_batch))
                .await
                .map_err(|e| {
                    tracing::error!("Failed to register subscriptions: {}", e);
                    e
                })?;

            // Step 2: Long-poll loop
            loop {
                let max_seq = *this.max_processed_sequence_id.lock();
                let poll_req = rpc::GcsSubscriberPollRequest {
                    subscriber_id: this.subscriber_id.clone(),
                    max_processed_sequence_id: max_seq,
                    publisher_id: vec![],
                };

                let poll_result = tokio::select! {
                    result = stub.gcs_subscriber_poll(tonic::Request::new(poll_req)) => result,
                    _ = this.shutdown.notified() => {
                        tracing::info!("GCS subscriber shutting down");
                        *this.is_running.lock() = false;
                        return Ok(());
                    }
                };

                match poll_result {
                    Ok(response) => {
                        let reply = response.into_inner();
                        if !reply.pub_messages.is_empty() {
                            this.dispatch_messages(reply.pub_messages);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("GCS subscriber poll failed: {}, retrying...", e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        })
    }

    /// Stop the polling loop.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Whether the polling loop is running.
    pub fn is_running(&self) -> bool {
        *self.is_running.lock()
    }

    /// Get the subscriber ID.
    pub fn subscriber_id(&self) -> &[u8] {
        &self.subscriber_id
    }

    /// Get the max processed sequence ID.
    pub fn max_processed_sequence_id(&self) -> i64 {
        *self.max_processed_sequence_id.lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use tokio::sync::broadcast;

    /// A minimal mock InternalPubSubGcsService for testing.
    struct MockPubSubService {
        /// Commands received via GcsSubscriberCommandBatch.
        commands_received: Mutex<Vec<rpc::Command>>,
        /// Channel for publishing messages to waiting polls.
        msg_tx: broadcast::Sender<rpc::PubMessage>,
        /// Sequence counter for published messages.
        sequence_counter: AtomicI64,
    }

    impl MockPubSubService {
        fn new() -> (Self, broadcast::Sender<rpc::PubMessage>) {
            let (tx, _) = broadcast::channel(128);
            let tx_clone = tx.clone();
            (
                Self {
                    commands_received: Mutex::new(Vec::new()),
                    msg_tx: tx,
                    sequence_counter: AtomicI64::new(1),
                },
                tx_clone,
            )
        }
    }

    #[tonic::async_trait]
    impl rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsService
        for MockPubSubService
    {
        async fn gcs_publish(
            &self,
            _request: tonic::Request<rpc::GcsPublishRequest>,
        ) -> Result<tonic::Response<rpc::GcsPublishReply>, Status> {
            Ok(tonic::Response::new(rpc::GcsPublishReply::default()))
        }

        async fn gcs_subscriber_poll(
            &self,
            request: tonic::Request<rpc::GcsSubscriberPollRequest>,
        ) -> Result<tonic::Response<rpc::GcsSubscriberPollReply>, Status> {
            let req = request.into_inner();
            let mut rx = self.msg_tx.subscribe();

            // Wait for a message
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Ok(mut msg)) => {
                    let seq = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
                    msg.sequence_id = seq;
                    // Only deliver if sequence_id > max_processed
                    if msg.sequence_id > req.max_processed_sequence_id {
                        Ok(tonic::Response::new(rpc::GcsSubscriberPollReply {
                            pub_messages: vec![msg],
                            publisher_id: vec![],
                            status: None,
                        }))
                    } else {
                        Ok(tonic::Response::new(rpc::GcsSubscriberPollReply::default()))
                    }
                }
                _ => Ok(tonic::Response::new(rpc::GcsSubscriberPollReply::default())),
            }
        }

        async fn gcs_subscriber_command_batch(
            &self,
            request: tonic::Request<rpc::GcsSubscriberCommandBatchRequest>,
        ) -> Result<tonic::Response<rpc::GcsSubscriberCommandBatchReply>, Status> {
            let req = request.into_inner();
            self.commands_received.lock().extend(req.commands);
            Ok(tonic::Response::new(
                rpc::GcsSubscriberCommandBatchReply::default(),
            ))
        }
    }

    /// Start a mock GCS pubsub server, returning (address, message_sender).
    async fn start_mock_server() -> (SocketAddr, broadcast::Sender<rpc::PubMessage>) {
        let (svc, msg_tx) = MockPubSubService::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(
                    rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsServiceServer::new(svc),
                )
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(50)).await;
        (addr, msg_tx)
    }

    #[test]
    fn test_subscriber_new() {
        let sub = GcsSubscriberClient::new(b"sub1".to_vec());
        assert_eq!(sub.subscriber_id(), b"sub1");
        assert_eq!(sub.max_processed_sequence_id(), 0);
        assert!(!sub.is_running());
    }

    #[test]
    fn test_subscribe_registers_callback() {
        let sub = GcsSubscriberClient::new(b"sub1".to_vec());
        sub.subscribe(3, vec![], Box::new(|_| {}));
        sub.subscribe(4, b"key1".to_vec(), Box::new(|_| {}));

        assert_eq!(sub.subscriptions.lock().len(), 2);
        assert_eq!(sub.channel_callbacks.lock().len(), 2);
    }

    #[test]
    fn test_build_command_batch() {
        let sub = GcsSubscriberClient::new(b"sub1".to_vec());
        sub.subscribe(3, vec![], Box::new(|_| {}));
        sub.subscribe(5, b"node1".to_vec(), Box::new(|_| {}));

        let batch = sub.build_command_batch();
        assert_eq!(batch.subscriber_id, b"sub1");
        assert_eq!(batch.commands.len(), 2);
        assert_eq!(batch.commands[0].channel_type, 3);
        assert_eq!(batch.commands[1].channel_type, 5);
        assert_eq!(batch.commands[1].key_id, b"node1");
    }

    #[test]
    fn test_dispatch_messages_invokes_callbacks() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let sub = GcsSubscriberClient::new(b"sub1".to_vec());
        sub.subscribe(
            3,
            vec![],
            Box::new(move |_msg| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );

        let messages = vec![
            rpc::PubMessage {
                channel_type: 3,
                key_id: b"a".to_vec(),
                sequence_id: 1,
                ..Default::default()
            },
            rpc::PubMessage {
                channel_type: 3,
                key_id: b"b".to_vec(),
                sequence_id: 2,
                ..Default::default()
            },
        ];

        sub.dispatch_messages(messages);
        assert_eq!(counter.load(Ordering::Relaxed), 2);
        assert_eq!(sub.max_processed_sequence_id(), 2);
    }

    #[test]
    fn test_dispatch_filters_by_key() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let sub = GcsSubscriberClient::new(b"sub1".to_vec());
        sub.subscribe(
            3,
            b"target".to_vec(),
            Box::new(move |_msg| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );

        let messages = vec![
            rpc::PubMessage {
                channel_type: 3,
                key_id: b"target".to_vec(),
                sequence_id: 1,
                ..Default::default()
            },
            rpc::PubMessage {
                channel_type: 3,
                key_id: b"other".to_vec(),
                sequence_id: 2,
                ..Default::default()
            },
        ];

        sub.dispatch_messages(messages);
        // Only the "target" key should trigger the callback
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_dispatch_channel_isolation() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let sub = GcsSubscriberClient::new(b"sub1".to_vec());
        sub.subscribe(
            3, // actor channel
            vec![],
            Box::new(move |_msg| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );

        // Publish to channel 4 (job) — should not trigger
        let messages = vec![rpc::PubMessage {
            channel_type: 4,
            key_id: b"job1".to_vec(),
            sequence_id: 1,
            ..Default::default()
        }];

        sub.dispatch_messages(messages);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_subscriber_poll_loop_receives_messages() {
        let (addr, msg_tx) = start_mock_server().await;

        let channel = Channel::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let stub = PubSubClient::new(channel);

        let received = Arc::new(AtomicUsize::new(0));
        let received_clone = received.clone();

        let sub = Arc::new(GcsSubscriberClient::new(b"sub1".to_vec()));
        sub.subscribe(
            3,
            vec![],
            Box::new(move |_msg| {
                received_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );

        let handle = sub.start(stub);

        // Give the polling loop time to register and start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish a message
        msg_tx
            .send(rpc::PubMessage {
                channel_type: 3,
                key_id: b"actor1".to_vec(),
                ..Default::default()
            })
            .unwrap();

        // Wait for delivery
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(received.load(Ordering::Relaxed), 1);

        // Shutdown
        sub.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn test_subscriber_multiple_messages() {
        let (addr, msg_tx) = start_mock_server().await;

        let channel = Channel::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let stub = PubSubClient::new(channel);

        let received = Arc::new(AtomicUsize::new(0));
        let received_clone = received.clone();

        let sub = Arc::new(GcsSubscriberClient::new(b"sub1".to_vec()));
        sub.subscribe(
            3,
            vec![],
            Box::new(move |_msg| {
                received_clone.fetch_add(1, Ordering::Relaxed);
            }),
        );

        let handle = sub.start(stub);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send multiple messages
        for i in 0..3 {
            msg_tx
                .send(rpc::PubMessage {
                    channel_type: 3,
                    key_id: format!("actor{i}").into_bytes(),
                    ..Default::default()
                })
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(received.load(Ordering::Relaxed), 3);

        sub.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    #[tokio::test]
    async fn test_subscriber_shutdown() {
        let (addr, _msg_tx) = start_mock_server().await;

        let channel = Channel::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let stub = PubSubClient::new(channel);

        let sub = Arc::new(GcsSubscriberClient::new(b"sub1".to_vec()));
        sub.subscribe(3, vec![], Box::new(|_| {}));

        let handle = sub.start(stub);
        tokio::time::sleep(Duration::from_millis(100)).await;

        sub.shutdown();
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "polling loop should stop after shutdown");
    }

    #[test]
    fn test_with_poll_timeout() {
        let sub = GcsSubscriberClient::new(b"sub1".to_vec())
            .with_poll_timeout(Duration::from_secs(5));
        assert_eq!(sub.poll_timeout, Duration::from_secs(5));
    }
}

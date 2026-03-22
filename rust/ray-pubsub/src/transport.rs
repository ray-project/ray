//! Subscriber transport driver.
//!
//! Encapsulates the production subscriber poll lifecycle:
//! drain commands → send to publisher → long-poll → dispatch/failure.
//!
//! C++ equivalent: `Subscriber::MakeLongPollingPubsubConnection` +
//! `HandleLongPollingResponse` + `SendCommandBatchIfPossible`.
//!
//! The transport driver takes a pluggable `SubscriberClient` trait so it can
//! be driven by a real gRPC client in production or an in-process publisher
//! in tests.

use std::sync::Arc;

use crate::publisher::PubMessage;
use crate::subscriber::{Subscriber, SubscriberCommand};

/// Error from the subscriber transport.
#[derive(Debug, Clone)]
pub enum TransportError {
    /// Failed to send command batch to publisher.
    CommandBatchFailed(String),
    /// Publisher is unreachable (owner death / connection lost).
    /// C++ equivalent: non-OK status from PubsubLongPolling RPC triggers
    /// HandlePublisherFailure for all channels.
    PublisherFailure,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommandBatchFailed(msg) => write!(f, "command batch failed: {}", msg),
            Self::PublisherFailure => write!(f, "publisher failure (owner death)"),
        }
    }
}

/// Trait for the transport layer to the owner/publisher.
/// In production: gRPC client connecting to the owner's CoreWorkerService.
/// In tests: in-process client wrapping the Publisher directly.
///
/// C++ equivalent: `SubscriberClientInterface` with
/// `PubsubLongPolling` and `PubsubCommandBatch` RPCs.
#[async_trait::async_trait]
pub trait SubscriberClient: Send + Sync {
    /// Send a batch of subscribe/unsubscribe commands to the publisher.
    /// C++ equivalent: `SubscriberClient::PubsubCommandBatch` RPC.
    async fn send_command_batch(
        &self,
        subscriber_id: &[u8],
        commands: &[SubscriberCommand],
    ) -> Result<(), String>;

    /// Long-poll for messages from the publisher.
    /// Returns (messages, publisher_id_from_response).
    /// C++ equivalent: `SubscriberClient::PubsubLongPolling` RPC.
    async fn long_poll(
        &self,
        subscriber_id: &[u8],
        max_processed_sequence_id: i64,
    ) -> Result<(Vec<PubMessage>, Vec<u8>), String>;
}

/// Production subscriber transport driver.
///
/// Drives the subscriber through the full command/poll/dispatch lifecycle.
/// This is the code that would run in a background tokio task per publisher.
///
/// C++ equivalent: the subscriber's poll loop that calls
/// `MakeLongPollingPubsubConnection` → `HandleLongPollingResponse` →
/// `SendCommandBatchIfPossible` in a cycle.
pub struct SubscriberTransport {
    subscriber: Arc<Subscriber>,
}

impl SubscriberTransport {
    /// Create a new transport driver for the given subscriber.
    pub fn new(subscriber: Arc<Subscriber>) -> Self {
        Self { subscriber }
    }

    /// Run one poll cycle for a publisher:
    ///
    /// 1. Drain pending commands from the subscriber and send them to the publisher
    /// 2. Long-poll the publisher for messages
    /// 3. On success: dispatch via subscriber.handle_poll_response()
    /// 4. On failure: invoke subscriber.handle_publisher_failure()
    ///
    /// C++ equivalent: one iteration of the subscriber's poll loop
    /// (SendCommandBatchIfPossible + MakeLongPollingPubsubConnection +
    /// HandleLongPollingResponse).
    ///
    /// Returns the number of messages processed, or TransportError on failure.
    pub async fn poll_publisher(
        &self,
        publisher_id: &[u8],
        client: &dyn SubscriberClient,
    ) -> Result<usize, TransportError> {
        // 1. Drain and send pending commands.
        // C++ equivalent: SendCommandBatchIfPossible
        let commands = self.subscriber.drain_commands(publisher_id);
        if !commands.is_empty() {
            if let Err(e) = client
                .send_command_batch(self.subscriber.subscriber_id(), &commands)
                .await
            {
                // Command batch failed — publisher is unreachable.
                // C++: command batch errors are logged but failure is detected
                // by the long-poll path. If we can't even send commands,
                // the publisher is dead.
                tracing::warn!("command batch failed for publisher: {}", e);
                self.subscriber.handle_publisher_failure(publisher_id);
                return Err(TransportError::PublisherFailure);
            }
        }

        // 2. Long-poll for messages.
        // C++ equivalent: MakeLongPollingPubsubConnection → HandleLongPollingResponse
        let max_seq = self.subscriber.max_processed_sequence_id(publisher_id);
        match client
            .long_poll(self.subscriber.subscriber_id(), max_seq)
            .await
        {
            Ok((messages, response_publisher_id)) => {
                // Dispatch to registered callbacks.
                let processed = self.subscriber.handle_poll_response(
                    publisher_id,
                    &response_publisher_id,
                    &messages,
                );
                Ok(processed)
            }
            Err(_) => {
                // Publisher failure — invoke failure callbacks for all subscriptions.
                // C++ equivalent: HandleLongPollingResponse with non-OK status
                // calls HandlePublisherFailure for all channels.
                self.subscriber.handle_publisher_failure(publisher_id);
                Err(TransportError::PublisherFailure)
            }
        }
    }

    /// Get a reference to the underlying subscriber.
    pub fn subscriber(&self) -> &Arc<Subscriber> {
        &self.subscriber
    }
}

/// In-process subscriber client that connects directly to a Publisher.
/// Used for testing without real gRPC networking.
///
/// C++ equivalent: MockWorkerClient in subscriber_test.cc.
pub struct InProcessSubscriberClient {
    publisher: Arc<crate::Publisher>,
    publisher_id: Vec<u8>,
}

impl InProcessSubscriberClient {
    /// Create a client backed by an in-process publisher.
    pub fn new(publisher: Arc<crate::Publisher>, publisher_id: Vec<u8>) -> Self {
        Self {
            publisher,
            publisher_id,
        }
    }
}

#[async_trait::async_trait]
impl SubscriberClient for InProcessSubscriberClient {
    async fn send_command_batch(
        &self,
        subscriber_id: &[u8],
        commands: &[SubscriberCommand],
    ) -> Result<(), String> {
        for cmd in commands {
            match cmd {
                SubscriberCommand::Subscribe {
                    channel_type,
                    key_id,
                } => {
                    self.publisher
                        .register_subscription(subscriber_id, *channel_type, key_id);
                }
                SubscriberCommand::Unsubscribe {
                    channel_type,
                    key_id,
                } => {
                    self.publisher
                        .unregister_subscription(subscriber_id, *channel_type, key_id);
                }
            }
        }
        Ok(())
    }

    async fn long_poll(
        &self,
        subscriber_id: &[u8],
        max_processed_sequence_id: i64,
    ) -> Result<(Vec<PubMessage>, Vec<u8>), String> {
        let rx = self
            .publisher
            .connect_subscriber(subscriber_id, max_processed_sequence_id);
        match rx.await {
            Ok(messages) => Ok((messages, self.publisher_id.clone())),
            Err(_) => Err("publisher dropped".to_string()),
        }
    }
}

/// A subscriber client that always fails (simulates unreachable publisher / owner death).
///
/// C++ equivalent: a long-poll that returns non-OK status.
pub struct FailingSubscriberClient;

#[async_trait::async_trait]
impl SubscriberClient for FailingSubscriberClient {
    async fn send_command_batch(
        &self,
        _subscriber_id: &[u8],
        _commands: &[SubscriberCommand],
    ) -> Result<(), String> {
        Err("publisher unreachable (owner dead)".to_string())
    }

    async fn long_poll(
        &self,
        _subscriber_id: &[u8],
        _max_processed_sequence_id: i64,
    ) -> Result<(Vec<PubMessage>, Vec<u8>), String> {
        Err("publisher unreachable (owner dead)".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Publisher, PublisherConfig, SubscriberConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_transport_poll_delivers_messages() {
        // Set up publisher (the "owner").
        let publisher = Arc::new(Publisher::new(PublisherConfig::default()));
        publisher.register_channel(0, false); // WORKER_OBJECT_EVICTION
        let publisher_id = b"owner_1".to_vec();

        // Set up subscriber.
        let subscriber = Arc::new(Subscriber::new(
            b"raylet_sub".to_vec(),
            SubscriberConfig::default(),
        ));

        // Track callback invocations.
        let callback_count = Arc::new(AtomicUsize::new(0));
        let cc = callback_count.clone();
        let item_cb: crate::subscriber::MessageCallback =
            Arc::new(move |_msg: &PubMessage| {
                cc.fetch_add(1, Ordering::Relaxed);
            });

        // Subscribe to channel 0 on the owner.
        subscriber.subscribe(&publisher_id, 0, b"obj_1", item_cb, None);

        // Create transport and client.
        let transport = SubscriberTransport::new(subscriber.clone());
        let client = InProcessSubscriberClient::new(publisher.clone(), publisher_id.clone());

        // We need the transport to: send commands → publisher registers → then
        // publish → then long-poll receives. Spawn a task that publishes after
        // a short delay (giving time for command batch to register the subscriber).
        let pub_clone = publisher.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            pub_clone.publish(0, b"obj_1", b"eviction".to_vec());
        });

        let processed = transport.poll_publisher(&publisher_id, &client).await.unwrap();
        assert_eq!(processed, 1, "one message dispatched via transport");
        assert_eq!(callback_count.load(Ordering::Relaxed), 1, "callback fired");
    }

    #[tokio::test]
    async fn test_transport_failure_invokes_publisher_failure() {
        let subscriber = Arc::new(Subscriber::new(
            b"raylet_sub".to_vec(),
            SubscriberConfig::default(),
        ));

        let failure_count = Arc::new(AtomicUsize::new(0));
        let fc = failure_count.clone();
        let failure_cb: crate::subscriber::FailureCallback =
            Arc::new(move |_key: &[u8]| {
                fc.fetch_add(1, Ordering::Relaxed);
            });
        let item_cb: crate::subscriber::MessageCallback =
            Arc::new(|_: &PubMessage| {});

        subscriber.subscribe(b"dead_owner", 0, b"obj_1", item_cb, Some(failure_cb));

        let transport = SubscriberTransport::new(subscriber.clone());
        let client = FailingSubscriberClient;

        let result = transport.poll_publisher(b"dead_owner", &client).await;
        assert!(result.is_err(), "should fail");
        assert_eq!(failure_count.load(Ordering::Relaxed), 1, "failure callback fired");
        assert!(!subscriber.is_subscribed(b"dead_owner", 0, b"obj_1"));
    }
}

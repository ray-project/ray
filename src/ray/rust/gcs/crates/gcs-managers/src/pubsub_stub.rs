//! Full implementation of InternalPubSubGcsService.
//!
//! Implements the per-subscriber mailbox + long-poll pattern used by the C++
//! Publisher. Replaces the earlier stub that returned empty responses.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Request, Response, Status};
use tracing::debug;

use gcs_proto::ray::rpc::command::CommandMessageOneOf;
use gcs_proto::ray::rpc::internal_pub_sub_gcs_service_server::InternalPubSubGcsService;
use gcs_proto::ray::rpc::*;
use gcs_pubsub::PubSubManager;

/// Long-poll timeout -- how long to hold the response before returning empty.
const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// PubSub service backed by a shared PubSubManager.
pub struct PubSubService {
    manager: Arc<PubSubManager>,
}

impl PubSubService {
    pub fn new(manager: Arc<PubSubManager>) -> Self {
        Self { manager }
    }
}

#[tonic::async_trait]
impl InternalPubSubGcsService for PubSubService {
    /// Relay published messages to matching subscribers.
    async fn gcs_publish(
        &self,
        req: Request<GcsPublishRequest>,
    ) -> Result<Response<GcsPublishReply>, Status> {
        let inner = req.into_inner();
        for msg in inner.pub_messages {
            self.manager.publish(msg);
        }
        Ok(Response::new(GcsPublishReply::default()))
    }

    /// Long-poll: hold the response until messages are available for this subscriber,
    /// or until the timeout expires.
    async fn gcs_subscriber_poll(
        &self,
        req: Request<GcsSubscriberPollRequest>,
    ) -> Result<Response<GcsSubscriberPollReply>, Status> {
        let inner = req.into_inner();
        let subscriber_id = inner.subscriber_id;
        let max_processed = inner.max_processed_sequence_id;

        let messages = self
            .manager
            .poll(&subscriber_id, max_processed, LONG_POLL_TIMEOUT)
            .await;

        Ok(Response::new(GcsSubscriberPollReply {
            pub_messages: messages,
            publisher_id: self.manager.publisher_id().to_vec(),
            status: None,
        }))
    }

    /// Process a batch of subscribe/unsubscribe commands.
    async fn gcs_subscriber_command_batch(
        &self,
        req: Request<GcsSubscriberCommandBatchRequest>,
    ) -> Result<Response<GcsSubscriberCommandBatchReply>, Status> {
        let inner = req.into_inner();
        let subscriber_id = inner.subscriber_id;

        for cmd in inner.commands {
            let channel_type = cmd.channel_type;
            let key_id = cmd.key_id;

            match cmd.command_message_one_of {
                Some(CommandMessageOneOf::SubscribeMessage(_sub_msg)) => {
                    self.manager
                        .subscribe_channel(&subscriber_id, channel_type, key_id);
                }
                Some(CommandMessageOneOf::UnsubscribeMessage(_)) => {
                    self.manager
                        .unsubscribe_channel(&subscriber_id, channel_type, key_id);
                }
                None => {
                    debug!(
                        channel_type,
                        "Received command with no subscribe/unsubscribe message"
                    );
                }
            }
        }

        Ok(Response::new(GcsSubscriberCommandBatchReply::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pubsub_service() -> PubSubService {
        let manager = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        PubSubService::new(manager)
    }

    #[test]
    fn test_pubsub_service_new() {
        let svc = make_pubsub_service();
        // Verify the service is created and the manager is accessible.
        assert_eq!(svc.manager.publisher_id(), b"test_pub");
    }

    #[tokio::test]
    async fn test_grpc_gcs_publish() {
        let manager = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        let svc = PubSubService::new(manager.clone());

        // Subscribe a subscriber to channel 3 (wildcard).
        manager.subscribe_channel(b"sub1", 3, vec![]);

        // Publish a message via gRPC.
        let msg = PubMessage {
            channel_type: 3,
            key_id: b"actor1".to_vec(),
            sequence_id: 0,
            inner_message: None,
        };
        svc.gcs_publish(Request::new(GcsPublishRequest {
            pub_messages: vec![msg],
        }))
        .await
        .unwrap();

        // Verify the subscriber received it.
        let msgs = manager
            .poll(b"sub1", 0, std::time::Duration::from_millis(100))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].key_id, b"actor1");
    }

    #[tokio::test]
    async fn test_grpc_gcs_subscriber_poll() {
        let manager = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        let svc = PubSubService::new(manager.clone());

        // Subscribe and publish directly on manager.
        manager.subscribe_channel(b"sub1", 3, vec![]);
        manager.publish(PubMessage {
            channel_type: 3,
            key_id: b"k1".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });

        // Poll via gRPC handler.
        let reply = svc
            .gcs_subscriber_poll(Request::new(GcsSubscriberPollRequest {
                subscriber_id: b"sub1".to_vec(),
                max_processed_sequence_id: 0,
                publisher_id: vec![],
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.pub_messages.len(), 1);
        assert_eq!(reply.pub_messages[0].key_id, b"k1");
        assert_eq!(reply.publisher_id, b"test_pub");
    }

    #[tokio::test]
    async fn test_grpc_gcs_subscriber_command_batch() {
        let manager = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        let svc = PubSubService::new(manager.clone());

        // Send a subscribe command via gRPC.
        let subscribe_cmd = Command {
            channel_type: 3,
            key_id: vec![],
            command_message_one_of: Some(CommandMessageOneOf::SubscribeMessage(
                SubMessage::default(),
            )),
        };
        svc.gcs_subscriber_command_batch(Request::new(
            GcsSubscriberCommandBatchRequest {
                subscriber_id: b"sub1".to_vec(),
                commands: vec![subscribe_cmd],
                sender_id: vec![],
            },
        ))
        .await
        .unwrap();

        // Now publish a message -- should be delivered.
        manager.publish(PubMessage {
            channel_type: 3,
            key_id: b"a1".to_vec(),
            sequence_id: 0,
            inner_message: None,
        });

        let msgs = manager
            .poll(b"sub1", 0, std::time::Duration::from_millis(100))
            .await;
        assert_eq!(msgs.len(), 1);
    }
}

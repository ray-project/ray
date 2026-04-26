//! GCS-side implementation of the `ray.rpc.syncer.RaySyncer` gRPC service.
//!
//! Maps the C++ `ray::syncer::RaySyncerService` wrapper around
//! `ray::syncer::RaySyncer` (`src/ray/ray_syncer/ray_syncer.h:213-236`
//! and `ray_syncer.cc:226-277`). The C++ GCS registers it in
//! `GcsServer::InitRaySyncer` (`src/ray/gcs/gcs_server.cc:607-621`) so
//! raylets can open a bidirectional stream to the GCS and exchange
//! `RESOURCE_VIEW` / `COMMANDS` messages.
//!
//! Scope of the Rust port
//! ----------------------
//! The full C++ implementation includes per-component version tracking,
//! batching, and authentication-token validation. The Rust server's job for
//! *service surface parity* is:
//!
//!   1. Accept inbound bidi streams on `/ray.rpc.syncer.RaySyncer/StartSync`.
//!   2. Derive the remote node id from the `node_id` request-metadata header
//!      (hex-encoded, matching C++ `ray_syncer_client.cc:42` and the server
//!      extraction in `ray_syncer_server.cc:27-32`).
//!   3. Publish the GCS node id back in initial response metadata, same key.
//!   4. Forward every received `RaySyncMessageBatch` through the configured
//!      handler; in production wiring this is `GcsResourceManager`, which
//!      deserialises `RESOURCE_VIEW` payloads and updates its node table
//!      (`gcs_resource_manager.cc:37-58`).
//!   5. Fan out messages received from one node to every other connected
//!      node (and optionally expose a broadcast API to the GCS itself so the
//!      server can push commands).
//!
//! This implementation does *not* track per-(node,message) versions or
//! deduplicate against the sending peer's known state the way the C++
//! `RaySyncerBidiReactorBase` does — that is an optimisation, not a
//! correctness property. Every non-loopback message is forwarded; stale
//! messages are harmless because each message carries the full snapshot.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use gcs_proto::ray::rpc::syncer::ray_syncer_server::RaySyncer;
use gcs_proto::ray::rpc::syncer::{MessageType, RaySyncMessageBatch, ResourceViewSyncMessage};

use crate::resource_manager::GcsResourceManager;

/// Trait implemented by anything that wants to observe inbound sync messages.
///
/// Production wires `GcsResourceManager` in as the consumer (matching
/// C++ `GcsResourceManager` registered as the receiver for `RESOURCE_VIEW`
/// and `COMMANDS` in `gcs_server.cc:616-619`).
pub trait SyncMessageConsumer: Send + Sync + 'static {
    /// Handle a single received sync message. Implementations must not block
    /// the caller — they should dispatch to their own executor if work is
    /// heavy. Mirrors C++ `ReceiverInterface::ConsumeSyncMessage`.
    fn consume(&self, message: &RaySyncMessage);
}

impl SyncMessageConsumer for GcsResourceManager {
    fn consume(&self, message: &RaySyncMessage) {
        match MessageType::try_from(message.message_type) {
            Ok(MessageType::ResourceView) => {
                // Deserialise the payload. `prost::Message::decode` does the
                // same work as C++ `ParseFromString`
                // (gcs_resource_manager.cc:50-51).
                match <ResourceViewSyncMessage as prost::Message>::decode(
                    message.sync_message.as_slice(),
                ) {
                    Ok(decoded) => {
                        self.consume_resource_view(message.node_id.clone(), decoded);
                    }
                    Err(e) => {
                        warn!(error = %e, "RaySyncer: failed to decode ResourceViewSyncMessage");
                    }
                }
            }
            Ok(MessageType::Commands) => {
                // C++ treats COMMANDS as a no-op on the GCS side today
                // (gcs_resource_manager.cc:47-48 — "COMMANDS channel is
                // currently unused"). Keep log-only parity.
                debug!(
                    node_id = ?message.node_id,
                    "RaySyncer: received COMMANDS message (currently no-op)"
                );
            }
            Err(_) => {
                warn!(
                    message_type = message.message_type,
                    "RaySyncer: unknown message_type"
                );
            }
        }
    }
}

// Re-exports so callers outside this module can reach the proto types
// without depending directly on gcs-proto's nested module paths.
pub use gcs_proto::ray::rpc::syncer::RaySyncMessage;

/// A connected bidi stream, identified by the remote node id.
///
/// We hold a send handle so the GCS can broadcast outbound batches to every
/// connected node (parity with C++ `RaySyncer::BroadcastMessage` pushing to
/// each `sync_reactors_` entry — `ray_syncer.cc:209-224`).
struct Reactor {
    tx: mpsc::Sender<Result<RaySyncMessageBatch, Status>>,
}

/// GCS-side RaySyncer service.
pub struct RaySyncerService {
    /// This GCS's node id in binary form. The syncer convention (both in C++
    /// and here) is that the GCS uses the all-zero 28-byte `kGCSNodeID`.
    local_node_id: Vec<u8>,

    /// Downstream consumer for every received message. Typically
    /// `GcsResourceManager`.
    consumer: Arc<dyn SyncMessageConsumer>,

    /// Map from remote node id (binary) → its outbound sender. Guarded by a
    /// plain `parking_lot::Mutex` because insertion / removal is infrequent
    /// and per-message fan-out already clones the handles under the lock.
    reactors: Arc<Mutex<HashMap<Vec<u8>, Reactor>>>,

    /// Channel capacity used for each reactor's outbound queue. Exposed so
    /// tests can drive back-pressure scenarios deterministically.
    channel_capacity: usize,
}

impl RaySyncerService {
    pub fn new(local_node_id: Vec<u8>, consumer: Arc<dyn SyncMessageConsumer>) -> Self {
        Self {
            local_node_id,
            consumer,
            reactors: Arc::new(Mutex::new(HashMap::new())),
            channel_capacity: 64,
        }
    }

    /// Override the outbound-channel capacity (testing only).
    #[cfg(test)]
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Number of currently connected remote nodes. Useful for tests and
    /// health checks.
    pub fn connected_node_count(&self) -> usize {
        self.reactors.lock().len()
    }

    /// Broadcast a message to every connected remote node except the
    /// originator. Parity with C++ `RaySyncer::BroadcastMessage`
    /// (`ray_syncer.cc:209-224`) which pushes to each reactor's sending
    /// queue. Dropped channels (closed receivers) are removed.
    pub fn broadcast(&self, message: RaySyncMessage) {
        let originator = message.node_id.clone();
        let batch = RaySyncMessageBatch {
            messages: vec![message],
        };

        let senders: Vec<(Vec<u8>, mpsc::Sender<Result<RaySyncMessageBatch, Status>>)> = {
            let guard = self.reactors.lock();
            guard
                .iter()
                .filter_map(|(id, r)| {
                    if id == &originator {
                        None
                    } else {
                        Some((id.clone(), r.tx.clone()))
                    }
                })
                .collect()
        };

        for (node_id, tx) in senders {
            // try_send: if the receiver is full or closed we drop this update
            // to that peer rather than stalling the publisher. The next
            // full-snapshot message supersedes whatever was lost (same
            // rationale as resource-view being idempotent snapshots).
            if let Err(e) = tx.try_send(Ok(batch.clone())) {
                debug!(
                    node_id = ?node_id,
                    error = %e,
                    "RaySyncer: dropping message for congested/closed peer"
                );
            }
        }
    }

    /// Extract the remote node id from request metadata.
    ///
    /// C++ client sets `node_id = NodeID::Hex()`
    /// (`ray_syncer_client.cc:42`). Server side reads & hex-decodes it
    /// (`ray_syncer_server.cc:27-32`). We mirror that here. An absent or
    /// malformed header fails the RPC with `InvalidArgument`, matching the
    /// C++ `RAY_CHECK` which aborts the process.
    fn remote_node_id_from_request<T>(request: &Request<T>) -> Result<Vec<u8>, Status> {
        let meta = request
            .metadata()
            .get("node_id")
            .ok_or_else(|| Status::invalid_argument("missing node_id metadata"))?;
        let hex = meta
            .to_str()
            .map_err(|_| Status::invalid_argument("node_id metadata is not ASCII"))?;
        hex_to_bytes(hex)
            .ok_or_else(|| Status::invalid_argument("node_id metadata is not valid hex"))
    }
}

fn hex_to_bytes(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = from_hex_digit(bytes[i])?;
        let lo = from_hex_digit(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

fn from_hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + b - b'a'),
        b'A'..=b'F' => Some(10 + b - b'A'),
        _ => None,
    }
}

fn bytes_to_hex(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for &byte in b {
        s.push(HEX[(byte >> 4) as usize] as char);
        s.push(HEX[(byte & 0x0f) as usize] as char);
    }
    s
}

/// Type alias for the generated response stream associated type.
pub type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<RaySyncMessageBatch, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl RaySyncer for RaySyncerService {
    type StartSyncStream = ResponseStream;

    async fn start_sync(
        &self,
        request: Request<Streaming<RaySyncMessageBatch>>,
    ) -> Result<Response<Self::StartSyncStream>, Status> {
        let remote_node_id = Self::remote_node_id_from_request(&request)?;
        let mut inbound = request.into_inner();

        let (tx, rx) = mpsc::channel::<Result<RaySyncMessageBatch, Status>>(self.channel_capacity);

        // Register the new reactor, replacing any previous connection from
        // the same node id. Parity with C++ `RaySyncerService::StartSync`
        // which calls `syncer_.Disconnect(GetRemoteNodeID())` before
        // `syncer_.Connect` (`ray_syncer.cc:272-275`).
        {
            let mut guard = self.reactors.lock();
            if let Some(prev) = guard.remove(&remote_node_id) {
                info!(
                    node = bytes_to_hex(&remote_node_id),
                    "RaySyncer: replacing existing reactor for reconnecting node"
                );
                drop(prev); // closes the old outbound channel
            }
            guard.insert(
                remote_node_id.clone(),
                Reactor { tx: tx.clone() },
            );
        }
        info!(
            node = bytes_to_hex(&remote_node_id),
            "RaySyncer: node connected"
        );

        // Spawn the reader: pull batches off the inbound stream, dispatch
        // each message to the consumer, and re-broadcast it to every other
        // connected peer.
        let consumer = self.consumer.clone();
        let reactors = self.reactors.clone();
        let local_node_id = self.local_node_id.clone();
        let owner_id = remote_node_id.clone();
        tokio::spawn(async move {
            while let Some(batch_res) = inbound.next().await {
                match batch_res {
                    Ok(batch) => {
                        for msg in &batch.messages {
                            if msg.node_id == local_node_id {
                                // Skip loopback — this is our own broadcast
                                // echoing back. Mirrors C++
                                // `RaySyncerBidiReactorBase::ReceiveUpdate`
                                // skip-self logic
                                // (`ray_syncer_bidi_reactor_base.h:69`).
                                continue;
                            }
                            consumer.consume(msg);
                        }

                        // Fan out to other connected reactors.
                        let senders: Vec<(
                            Vec<u8>,
                            mpsc::Sender<Result<RaySyncMessageBatch, Status>>,
                        )> = {
                            let guard = reactors.lock();
                            guard
                                .iter()
                                .filter_map(|(id, r)| {
                                    if id == &owner_id {
                                        None
                                    } else {
                                        Some((id.clone(), r.tx.clone()))
                                    }
                                })
                                .collect()
                        };
                        for (peer, peer_tx) in senders {
                            if let Err(e) = peer_tx.try_send(Ok(batch.clone())) {
                                debug!(
                                    peer = bytes_to_hex(&peer),
                                    error = %e,
                                    "RaySyncer: dropping fan-out to congested/closed peer"
                                );
                            }
                        }
                    }
                    Err(status) => {
                        warn!(
                            node = bytes_to_hex(&owner_id),
                            code = ?status.code(),
                            "RaySyncer: inbound stream error"
                        );
                        break;
                    }
                }
            }

            // Stream ended — remove this reactor. Parity with the
            // `cleanup_cb` installed by `RaySyncerService::StartSync`
            // (`ray_syncer.cc:233-255`) which erases from `sync_reactors_`
            // when OnDone fires.
            let mut guard = reactors.lock();
            if let Some(existing) = guard.get(&owner_id) {
                // Only remove if the stored reactor is still ours. A faster
                // reconnect may have swapped in a new reactor under the
                // same node id.
                if existing.tx.same_channel(&tx) {
                    guard.remove(&owner_id);
                    info!(
                        node = bytes_to_hex(&owner_id),
                        "RaySyncer: node disconnected"
                    );
                }
            }
        });

        // Tonic does not expose a direct "send initial metadata" API from
        // inside the handler; returning the response attaches headers below.
        // C++ sends `node_id` back in initial metadata
        // (`ray_syncer_server.cc:79`). We carry the same header on the
        // response so clients that look it up (e.g. the C++
        // `RayClientBidiReactor`) see the GCS node id.
        let stream: ResponseStream = Box::pin(ReceiverStream::new(rx));
        let mut response = Response::new(stream);
        response
            .metadata_mut()
            .insert(
                "node_id",
                bytes_to_hex(&self.local_node_id)
                    .parse()
                    .expect("hex is always valid ASCII for metadata"),
            );
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_proto::ray::rpc::node_resource_info_gcs_service_server::NodeResourceInfoGcsService;
    use gcs_proto::ray::rpc::syncer::{MessageType, RaySyncMessage};

    /// Helper: build a RESOURCE_VIEW message carrying the given totals for
    /// the given node.
    fn resource_view_msg(node_id: &[u8], total_cpu: f64) -> RaySyncMessage {
        let mut rv = ResourceViewSyncMessage::default();
        rv.resources_total.insert("CPU".to_string(), total_cpu);
        let payload = prost::Message::encode_to_vec(&rv);
        RaySyncMessage {
            version: 1,
            message_type: MessageType::ResourceView as i32,
            sync_message: payload,
            node_id: node_id.to_vec(),
        }
    }

    #[test]
    fn hex_roundtrip() {
        let bytes = vec![0x00, 0xab, 0xcd, 0xef, 0x12, 0x34];
        let hex = bytes_to_hex(&bytes);
        assert_eq!(hex, "00abcdef1234");
        assert_eq!(hex_to_bytes(&hex), Some(bytes));
    }

    #[test]
    fn hex_rejects_odd_length() {
        assert!(hex_to_bytes("abc").is_none());
    }

    #[test]
    fn hex_rejects_non_hex() {
        assert!(hex_to_bytes("zz").is_none());
    }

    #[tokio::test]
    async fn resource_manager_consumes_resource_view() {
        let rm = Arc::new(GcsResourceManager::new());
        let msg = resource_view_msg(b"remote-node-1", 8.0);
        rm.consume(&msg);

        let reply = rm
            .get_all_total_resources(Request::new(
                gcs_proto::ray::rpc::GetAllTotalResourcesRequest {},
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.resources_list.len(), 1);
        assert_eq!(
            reply.resources_list[0].resources_total.get("CPU"),
            Some(&8.0)
        );
        assert_eq!(reply.resources_list[0].node_id, b"remote-node-1");
    }

    #[tokio::test]
    async fn resource_manager_ignores_unknown_payload() {
        let rm = Arc::new(GcsResourceManager::new());
        // COMMANDS is a no-op; should not touch the resource table.
        let mut cmd = RaySyncMessage::default();
        cmd.message_type = MessageType::Commands as i32;
        cmd.node_id = b"someone".to_vec();
        rm.consume(&cmd);

        let reply = rm
            .get_all_total_resources(Request::new(
                gcs_proto::ray::rpc::GetAllTotalResourcesRequest {},
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.resources_list.is_empty());
    }

    #[tokio::test]
    async fn resource_view_draining_tracked_and_cleared() {
        let rm = GcsResourceManager::new();

        // Draining on
        let mut drain = ResourceViewSyncMessage::default();
        drain.is_draining = true;
        drain.draining_deadline_timestamp_ms = 42;
        rm.consume_resource_view(b"n".to_vec(), drain);

        // Draining off — deadline 0, is_draining false
        let clear = ResourceViewSyncMessage::default();
        rm.consume_resource_view(b"n".to_vec(), clear);

        // The draining map should be empty now.
        let reply = rm
            .get_draining_nodes(Request::new(
                gcs_proto::ray::rpc::GetDrainingNodesRequest {},
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(reply.draining_nodes.is_empty());
    }

    #[tokio::test]
    async fn start_sync_requires_node_id_metadata() {
        let rm = Arc::new(GcsResourceManager::new());
        let svc = RaySyncerService::new(vec![0u8; 28], rm);

        // Build an empty streaming request *without* the node_id metadata.
        // We can't easily construct a `Streaming<T>` directly, so instead
        // we verify the extractor returns InvalidArgument on the raw
        // request.
        let req: Request<()> = Request::new(());
        let err = RaySyncerService::remote_node_id_from_request(&req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // Confirm a well-formed hex header is accepted.
        let mut req: Request<()> = Request::new(());
        req.metadata_mut()
            .insert("node_id", "deadbeef".parse().unwrap());
        let bytes = RaySyncerService::remote_node_id_from_request(&req).unwrap();
        assert_eq!(bytes, vec![0xde, 0xad, 0xbe, 0xef]);

        // Bound svc so the compiler doesn't warn about unused binding on
        // failure paths above.
        assert_eq!(svc.connected_node_count(), 0);
    }

    #[tokio::test]
    async fn broadcast_skips_originator_and_delivers_to_others() {
        let rm = Arc::new(GcsResourceManager::new());
        let svc = RaySyncerService::new(vec![0u8; 28], rm).with_channel_capacity(4);

        let (tx_a, mut rx_a) = mpsc::channel(4);
        let (tx_b, mut rx_b) = mpsc::channel(4);
        svc.reactors
            .lock()
            .insert(b"A".to_vec(), Reactor { tx: tx_a });
        svc.reactors
            .lock()
            .insert(b"B".to_vec(), Reactor { tx: tx_b });

        // A message originating from node "A" should reach B but not A.
        svc.broadcast(resource_view_msg(b"A", 4.0));

        assert!(rx_a.try_recv().is_err(), "A must not receive its own msg");
        let delivered = rx_b.recv().await.unwrap().unwrap();
        assert_eq!(delivered.messages.len(), 1);
        assert_eq!(delivered.messages[0].node_id, b"A");
    }
}

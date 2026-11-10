from ray.serve._private.constants import SERVE_INGRESS_ROUTER_HEADER_PREFIX
from ray.serve._private.constants_utils import (
    get_env_float_positive,
    get_env_int_positive,
)

# choose_replica kwarg carrying the prompt token IDs to KV-aware routers.
REQUEST_TOKEN_IDS_KWARG = "request_token_ids"

# Internal metadata for moving pre-routing prompt token IDs from the selected
# LLMRouter replica to the selected LLMServer replica. The payload itself is
# sent over a replica-local ZMQ side channel; HAProxy only forwards the lookup
# key as a request header.
KV_TOKEN_KEY_HEADER = SERVE_INGRESS_ROUTER_HEADER_PREFIX + "kv-token-key"
# routing_stats key under which each LLMServer advertises its ZMQ token endpoint
# to LLMRouter.
KV_TOKEN_METADATA_KEY = "kv_token_metadata"

# P/D: prefill's KV transfer params (base64 JSON) for the selected decode replica.
KV_TRANSFER_PARAMS_HEADER = SERVE_INGRESS_ROUTER_HEADER_PREFIX + "kv-transfer-params"
# P/D: the backend application the P/D router application routes to.
PD_BACKEND_APP_NAME = "pd-backend"

# Router config of each P/D stage's selection service. Cost per replica, lowest
# wins:
# - Prefill: queued prefill tokens plus the request's uncached tokens. The scale
#   makes them outweigh booked KV blocks, which only break ties.
# - Decode: booked KV blocks only. D pulls the prompt's KV from P, so cache
#   overlap earns no credit and the prompt books no prefill load.
# The selection service reads them from DYN_ROUTER_* env vars once, at
# construction; its replica mesh carries each booking's prefill tracking to peers.
# TODO (jeffreywang): Pass these to SelectionService directly once it accepts a
# router config, and disable active-block tracking for prefill instead of
# scaling its load once SelectionService honors that.
PREFILL_SELECTION_SVC_ENV = {
    "DYN_ROUTER_PREFILL_LOAD_SCALE": "1000000",  # Prefill cost outweighs KV blocks.
}
DECODE_SELECTION_SVC_ENV = {
    "DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT": "0",  # No credit for KV cache overlap.
    "DYN_ROUTER_ASSUME_KV_REUSE": "false",  # Book all prompt blocks as new load.
    "DYN_ROUTER_TRACK_PREFILL_TOKENS": "false",  # Prompt tokens add no prefill load.
}

# Prompt-token channel resource bounds. Sending is best effort: if a ZMQ
# pipe is unavailable or backed up, the router omits the token key and the
# engine falls back to normal tokenization.
KV_TOKEN_STAGING_TTL_S = get_env_float_positive(
    "RAY_SERVE_LLM_KV_TOKEN_STAGING_TTL_S", 60
)
KV_TOKEN_STAGING_MAX_ENTRIES = get_env_int_positive(
    "RAY_SERVE_LLM_KV_TOKEN_STAGING_MAX_ENTRIES", 8192
)
KV_TOKEN_STAGING_MAX_BYTES = get_env_int_positive(
    "RAY_SERVE_LLM_KV_TOKEN_STAGING_MAX_BYTES", 1024**3
)
KV_TOKEN_ZMQ_SEND_QUEUE_LIMIT = 256
KV_TOKEN_ZMQ_RECEIVE_QUEUE_LIMIT = 1024
KV_TOKEN_ZMQ_MAX_SOCKETS = 4096

# experimental_configs key overriding the per-node base port.
KV_EVENTS_PORT_BASE_KEY = "KV_EVENTS_PORT_BASE"
DEFAULT_KV_EVENTS_PORT_BASE = 5557

# experimental_configs key overriding the per-node base port for the prompt-token
# ZMQ PULL sockets. Defaults above the KV-events PUB and replay port ranges.
KV_TOKEN_PORT_BASE_KEY = "KV_TOKEN_PORT_BASE"
DEFAULT_KV_TOKEN_PORT_BASE = 7557

# experimental_configs key overriding the selection service's KV-indexer thread count.
KV_INDEXER_THREADS_KEY = "KV_INDEXER_THREADS"
DEFAULT_KV_INDEXER_THREADS = 4

# The engine's KV-event replay (ROUTER) socket sits this many ports above its PUB
# port, a separate range so it never collides with the PUB ports of colocated
# replicas (PORT_BASE + replica rank). Dynamo's selection service dials it to recover
# events missed before its SUB connected.
DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET = 1000

# TTL for a request's lifecycle tracking on the KV router actor. A live
# replica whose completion event was lost (e.g. a batch dropped on a
# transient actor outage) would otherwise leave its entry tracked forever.
REQUEST_TRACKING_TTL_S = 3600

# Session pinning idle timeout in seconds.
DEFAULT_SESSION_AFFINITY_TTL_S = 1800.0

# Soft affinity rebinds a session when Serve excludes its bound worker from the
# request's candidates; hard affinity would reject the request instead.
SESSION_AFFINITY_MODE = "soft"

# routing_stats key under which an LLMRouter advertises its replica-mesh
# endpoints, one per tracked LLMServer deployment: {deployment name: endpoint}.
KV_REPLICA_SYNC_METADATA_KEY = "kv_replica_sync_metadata"
# Header carrying the selecting LLMRouter's lifecycle endpoint to the LLMServer.
KV_LIFECYCLE_OWNER_HEADER = SERVE_INGRESS_ROUTER_HEADER_PREFIX + "kv-lifecycle-owner"

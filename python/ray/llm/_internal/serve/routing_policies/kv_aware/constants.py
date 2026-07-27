from ray.serve._private.constants_utils import (
    get_env_float_non_negative,
    get_env_int_non_negative,
)

# choose_replica kwarg carrying the prompt token IDs to KV-aware routers.
REQUEST_TOKEN_IDS_KWARG = "request_token_ids"

# Internal metadata for moving pre-routing prompt token IDs from the selected
# LLMRouter replica to the selected LLMServer replica. The payload itself is
# sent over a replica-local ZMQ side channel; HAProxy only forwards the lookup
# key as a request header.
KV_TOKEN_KEY_HEADER = "x-kv-token-key"
KV_TOKEN_METADATA_KEY = "kv_token_metadata"

# Prompt-token channel resource bounds. Sending is best effort: if a ZMQ
# pipe is unavailable or backed up, the router omits the token key and the
# engine falls back to normal tokenization.
KV_TOKEN_STAGING_TTL_S = get_env_float_non_negative(
    "RAY_SERVE_KV_TOKEN_STAGING_TTL_S", 60
)
KV_TOKEN_STAGING_MAX_ENTRIES = get_env_int_non_negative(
    "RAY_SERVE_KV_TOKEN_STAGING_MAX_ENTRIES", 8192
)
KV_TOKEN_STAGING_MAX_BYTES = get_env_int_non_negative(
    "RAY_SERVE_KV_TOKEN_STAGING_MAX_BYTES", 1024**3
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

# Bound best-effort lifecycle broadcast delivery so one slow ingress replica
# cannot stall an engine replica's whole lifecycle-event queue indefinitely.
LIFECYCLE_EVENT_BROADCAST_TIMEOUT_S = 3

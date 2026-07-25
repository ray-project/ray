# choose_replica kwarg carrying the prompt token IDs to KV-aware routers.
REQUEST_TOKEN_IDS_KWARG = "request_token_ids"

# Internal endpoint and headers for moving pre-routing prompt token IDs from the
# selected LLMRouter replica to the selected LLMServer replica. The request body
# is the token-id vector encoded as little-endian uint32 values.
KV_PROMPT_TOKEN_SIDE_CHANNEL_PATH = "/__ray_llm/kv_prompt_tokens"
KV_PROMPT_TOKEN_KEY_HEADER = "x-kv-token-key"
KV_PROMPT_TOKEN_LEN_HEADER = "x-kv-token-len"
KV_PROMPT_TOKEN_CRC32_HEADER = "x-kv-token-crc32"

# JSON fields returned by LLMRouter /internal/route and consumed by HAProxy Lua.
KV_PROMPT_TOKEN_KEY_FIELD = "kv_token_key"
KV_PROMPT_TOKEN_LEN_FIELD = "kv_token_len"
KV_PROMPT_TOKEN_CRC32_FIELD = "kv_token_crc32"

# experimental_configs key overriding the per-node base port.
KV_EVENTS_PORT_BASE_KEY = "KV_EVENTS_PORT_BASE"
DEFAULT_KV_EVENTS_PORT_BASE = 5557

# experimental_configs key overriding the selection service's KV-indexer thread count.
KV_INDEXER_THREADS_KEY = "KV_INDEXER_THREADS"
DEFAULT_KV_INDEXER_THREADS = 4

# experimental_configs key enabling select-time reservation. When enabled, the
# router uses Dynamo's select_and_reserve path and the engine reports only
# prefill-complete / request-complete lifecycle events.
KV_SELECT_RESERVE_KEY = "KV_SELECT_RESERVE"

# The engine's KV-event replay (ROUTER) socket sits this many ports above its PUB
# port, a separate range so it never collides with the PUB ports of colocated
# replicas (PORT_BASE + replica rank). Dynamo's selection service dials it to recover
# events missed before its SUB connected.
DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET = 1000

# TTL for a request's lifecycle tracking on the KV router actor. A live
# replica whose completion event was lost (e.g. a batch dropped on a
# transient actor outage) would otherwise leave its entry tracked forever.
REQUEST_TRACKING_TTL_S = 3600

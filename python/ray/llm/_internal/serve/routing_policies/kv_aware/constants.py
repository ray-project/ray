# experimental_configs key overriding the per-node base port.
KV_EVENTS_PORT_BASE_KEY = "KV_EVENTS_PORT_BASE"
DEFAULT_KV_EVENTS_PORT_BASE = 5557

# experimental_configs key overriding the selection service's KV-indexer thread count.
KV_INDEXER_THREADS_KEY = "KV_INDEXER_THREADS"
DEFAULT_KV_INDEXER_THREADS = 4

# The engine's KV-event replay (ROUTER) socket sits this many ports above its PUB
# port, a separate range so it never collides with the PUB ports of colocated
# replicas (PORT_BASE + replica rank). Dynamo's selection service dials it to recover
# events missed before its SUB connected.
DEFAULT_KV_EVENTS_REPLAY_PORT_OFFSET = 1000

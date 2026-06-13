# experimental_configs key overriding the per-node base port.
KV_EVENTS_PORT_BASE_KEY = "KV_EVENTS_PORT_BASE"
DEFAULT_KV_EVENTS_PORT_BASE = 5557

# Root directory of Dynamo's file discovery store; private per process. Set
# on both the replica (KvEventPublisher) and the router actor (KvRouter)
# before the DistributedRuntime is created.
DYN_FILE_KV_ENV = "DYN_FILE_KV"
KV_EVENT_PLANE_ENV_DEFAULTS = {
    "DYN_EVENT_PLANE": "zmq",
}

# With a broker URL set, Dynamo's event plane runs in ZMQ broker mode: every
# publisher connects its PUB socket to the broker's XSUB side and every
# subscriber its SUB socket to the XPUB side, with no discovery involved.
DYN_ZMQ_BROKER_URL_ENV = "DYN_ZMQ_BROKER_URL"

# The deployment-scoped Dynamo component endpoint, forming the endpoint path
# `<namespace>.backend.generate` on both sides. The component segment scopes
# everything the deployment exchanges over Dynamo:
# - event subject `namespace.<ns>.component.backend.kv-events`: the KV cache
#   event stream (the only subject carrying traffic today);
# - subjects `kv_metrics`, `active_sequences_events`, `forward-pass-metrics`:
#   load/sync metrics, idle until the Dynamo scheduling path is wired;
# - request-plane endpoints `backend/worker_kv_indexer_query_dp*`: the
#   workers' local-indexer query services backing recovery.
KV_EVENTS_ENDPOINT_SUFFIX = "backend.generate"

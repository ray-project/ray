import os

ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT = int(
    os.getenv("RAYLLM_ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT", "1")
)


# Timeout before download in multiplex deployment fails. <=0 means no timeout.
DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S = float(
    os.getenv("DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S", "30")
)
if DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S <= 0:
    DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S = None


# Number of retries for downloading a model in multiplex deployment.
DEFAULT_MULTIPLEX_DOWNLOAD_TRIES = int(
    os.getenv("DEFAULT_MULTIPLEX_DOWNLOAD_RETRIES", "3")
)


# If true, a default runtime_env will be injected to import rayllm on worker startup.
# This is a startup time optimization to avoid the latency penalty of sequentially
# importing rayllm in multiple layers of worker processes.
ENABLE_WORKER_PROCESS_SETUP_HOOK = (
    os.environ.get("RAYLLM_ENABLE_WORKER_PROCESS_SETUP_HOOK", "1") == "1"
)


CLOUD_OBJECT_MISSING_EXPIRE_S = 30
CLOUD_OBJECT_EXISTS_EXPIRE_S = 60 * 60

# Sentinel object used to indicate that a LoRA adapter config file is missing.
LORA_ADAPTER_CONFIG_NAME = "adapter_config.json"

DEFAULT_HEALTH_CHECK_PERIOD_S = int(
    os.getenv("RAY_SERVE_LLM_DEFAULT_HEALTH_CHECK_PERIOD_S", "10")
)
DEFAULT_HEALTH_CHECK_TIMEOUT_S = int(
    os.getenv("RAY_SERVE_LLM_DEFAULT_HEALTH_CHECK_TIMEOUT_S", "10")
)
DEFAULT_MAX_ONGOING_REQUESTS = int(
    os.getenv("RAY_SERVE_LLM_DEFAULT_MAX_ONGOING_REQUESTS", str(int(1e9)))
)
DEFAULT_MAX_REPLICAS = int(os.getenv("RAY_SERVE_LLM_DEFAULT_MAX_REPLICAS", "10"))
DEFAULT_MAX_TARGET_ONGOING_REQUESTS = int(
    os.getenv("RAY_SERVE_LLM_DEFAULT_MAX_TARGET_ONGOING_REQUESTS", str(int(1e9)))
)


ENGINE_START_TIMEOUT_S = int(os.getenv("RAYLLM_ENGINE_START_TIMEOUT_S", str(60 * 60)))

MIN_NUM_TOPLOGPROBS_ALLOWED = 0
MAX_NUM_TOPLOGPROBS_ALLOWED = 5
MODEL_RESPONSE_BATCH_TIMEOUT_MS = float(
    os.getenv("RAYLLM_MODEL_RESPONSE_BATCH_TIMEOUT_MS", "50")
)
RAYLLM_ENABLE_REQUEST_PROMPT_LOGS = (
    os.environ.get("RAYLLM_ENABLE_REQUEST_PROMPT_LOGS", "1") == "1"
)
RAYLLM_GUIDED_DECODING_BACKEND = os.environ.get(
    "RAYLLM_GUIDED_DECODING_BACKEND", "xgrammar"
)

MAX_NUM_STOPPING_SEQUENCES = int(os.getenv("RAYLLM_MAX_NUM_STOPPING_SEQUENCES", "8"))
ENV_VARS_TO_PROPAGATE = {
    "HUGGING_FACE_HUB_TOKEN",
    "HF_TOKEN",
}
# timeout in 10 minutes. Streaming can take longer than 3 min
DEFAULT_LLM_ROUTER_HTTP_TIMEOUT = float(
    os.environ.get("RAY_SERVE_LLM_ROUTER_HTTP_TIMEOUT", 600)
)

ENABLE_VERBOSE_TELEMETRY = bool(int(os.getenv("RAYLLM_ENABLE_VERBOSE_TELEMETRY", "0")))

RAYLLM_VLLM_ENGINE_CLS_ENV = "RAYLLM_VLLM_ENGINE_CLS"

# The ratio of number of router replicas to number of model replicas.
# Default to 2 meaning that there are 2 router replicas for every model replica.
DEFAULT_ROUTER_TO_MODEL_REPLICA_RATIO = float(
    os.getenv("RAY_SERVE_LLM_ROUTER_TO_MODEL_REPLICA_RATIO", "2")
)

DEFAULT_LLM_ROUTER_MIN_REPLICAS = int(
    os.environ.get("RAY_SERVE_LLM_ROUTER_MIN_REPLICAS", 2)
)
DEFAULT_LLM_ROUTER_INITIAL_REPLICAS = int(
    os.environ.get("RAY_SERVE_LLM_ROUTER_INITIAL_REPLICAS", 2)
)
DEFAULT_LLM_ROUTER_MAX_REPLICAS = int(
    os.environ.get("RAY_SERVE_LLM_ROUTER_MAX_REPLICAS", 1000)
)
DEFAULT_LLM_ROUTER_TARGET_ONGOING_REQUESTS = int(
    os.environ.get(
        "RAY_SERVE_LLM_ROUTER_TARGET_ONGOING_REQUESTS",
        DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    )
)


# HOME DIR
RAYLLM_HOME_DIR = os.environ.get("RAYLLM_HOME_DIR", os.path.expanduser("~/.ray/llm"))

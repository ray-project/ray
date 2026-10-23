from ray._common.utils import env_bool

# Default max_concurrency option in @ray.remote for async actors.
DEFAULT_MAX_CONCURRENCY_ASYNC = 1000

LOGGING_ROTATE_BYTES = 512 * 1024 * 1024  # 512MB.
LOGGING_ROTATE_BACKUP_COUNT = 5  # 5 Backup files at max.

# Redact `runtime_env` secrets (e.g. `env_vars` values) in dashboard responses to
# browser-originated requests and in events sent to an external HTTP endpoint.
# Set to 0 to keep the plaintext values instead.
RAY_DASHBOARD_REDACT_RUNTIME_ENV = env_bool("RAY_DASHBOARD_REDACT_RUNTIME_ENV", True)

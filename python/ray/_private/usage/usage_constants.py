SCHEMA_VERSION = "0.1"

# The key to store / obtain cluster metadata.
CLUSTER_METADATA_KEY = b"CLUSTER_METADATA"

# The name of a json file where usage stats will be written.
USAGE_STATS_FILE = "usage_stats.json"

USAGE_STATS_ENABLED_ENV_VAR = "RAY_USAGE_STATS_ENABLED"

USAGE_STATS_ENABLED_FOR_CLI_MESSAGE = (
    "Usage stats collection is enabled. To disable this, add `--disable-usage-stats` "
    "to the command that starts the cluster, or run the following command:"
    " `ray disable-usage-stats` before starting the cluster. "
    "See https://docs.ray.io/en/master/cluster/usage-stats.html for more details."
)

USAGE_STATS_ENABLED_FOR_RAY_INIT_MESSAGE = (
    "Usage stats collection is enabled. To disable this, run the following command:"
    " `ray disable-usage-stats` before starting Ray. "
    "See https://docs.ray.io/en/master/cluster/usage-stats.html for more details."
)

USAGE_STATS_DISABLED_MESSAGE = "Usage stats collection is disabled."

USAGE_STATS_ENABLED_BY_DEFAULT_FOR_CLI_MESSAGE = (
    "Usage stats collection is enabled by default without user confirmation "
    "because this terminal is detected to be non-interactive. "
    "To disable this, add `--disable-usage-stats` to the command that starts "
    "the cluster, or run the following command:"
    " `ray disable-usage-stats` before starting the cluster. "
    "See https://docs.ray.io/en/master/cluster/usage-stats.html for more details."
)

USAGE_STATS_ENABLED_BY_DEFAULT_FOR_RAY_INIT_MESSAGE = (
    "Usage stats collection is enabled by default for nightly wheels. "
    "To disable this, run the following command:"
    " `ray disable-usage-stats` before starting Ray. "
    "See https://docs.ray.io/en/master/cluster/usage-stats.html for more details."
)

USAGE_STATS_CONFIRMATION_MESSAGE = (
    "Enable usage stats collection? "
    "This prompt will auto-proceed in 10 seconds to avoid blocking cluster startup."
)

LIBRARY_USAGE_PREFIX = "library_usage_"

EXTRA_USAGE_TAG_PREFIX = "extra_usage_tag_"

USAGE_STATS_NAMESPACE = "usage_stats"

KUBERNETES_SERVICE_HOST_ENV = "KUBERNETES_SERVICE_HOST"
KUBERAY_ENV = "RAY_USAGE_STATS_KUBERAY_IN_USE"

PROVIDER_KUBERNETES_GENERIC = "kubernetes"
PROVIDER_KUBERAY = "kuberay"

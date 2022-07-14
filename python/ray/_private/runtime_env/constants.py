# Env var set by job manager to pass runtime env and metadata to subprocess
RAY_JOB_CONFIG_JSON_ENV_VAR = "RAY_JOB_CONFIG_JSON_ENV_VAR"

# The plugins which should be loaded when ray cluster starts.
RAY_RUNTIME_ENV_PLUGINS_ENV_VAR = "RAY_RUNTIME_ENV_PLUGINS"

# The schema files or directories of plugins which should be loaded in workers.
RAY_RUNTIME_ENV_PLUGIN_SCHEMAS_ENV_VAR = "RAY_RUNTIME_ENV_PLUGIN_SCHEMAS"

# The file suffix of runtime env plugin schemas.
RAY_RUNTIME_ENV_PLUGIN_SCHEMA_SUFFIX = ".json"

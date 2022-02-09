# Schema version of the usage stats.
# {
#   // version of the schema
#   “_version”: string,
#   // oss or product
#   “_source”: string,
#   “session_id”: string,
#   “timestamp_ms”: long
#   “ray_version”: string,
#   “git_commit”: string,
#   “os”: string,
#   “python_version”: string,
# }
SCHEMA_VERSION = "0.1"
schema = {
    # Version of the schema.
    "schema_version": str,
    # The source of the usage data. OSS by default.
    "source": str,
    # 128 bytes random uuid. It is 1:1 mapping to
    # session_name.
    "session_id": str,
    # The timestamp when data is "reported".
    "collect_timestamp_ms": int,
    # Ray version.
    "ray_version": str,
    # Ray commit hash.
    "git_commit": str,
    # The operating systems.
    "os": str,
    # Python version.
    "python_version": str,
}

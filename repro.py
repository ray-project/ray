import ray
data = {"a": 1}
dir = "/tmp/ray/session_latest"

ray._private.usage.usage_lib.write_usage_data(data, dir)

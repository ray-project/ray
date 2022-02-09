import pytest
import sys
import ray
import pathlib
import json

import ray._private.usage.usage_lib as ray_usage_lib
import ray._private.usage.usage_constants as usage_constants

from ray._private.test_utils import wait_for_condition
from ray import serve

# TODO(sang): Remove the hand-written schema validation once
# the public schema is published via Pypi.
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


def _validate_schema(payload: dict):
    """Perform the lightweight json schema validation.

    TODO(sang): Use protobuf for schema validation instead.

    Params:
        payload (dict): The payload to validate schema.

    Raises:
        ValueError if the schema validation fails.
    """
    for k, v in payload.items():
        if k not in schema:
            raise ValueError(
                f"Given payload doesn't contain a key {k} that's required "
                f"for the schema.\nschema: {schema}\npayload: {payload}"
            )
        if schema[k] != type(v):
            raise ValueError(
                f"The key {k}'s value {v} has an incorrect type. "
                f"key type: {type(k)}. schema type: {schema[k]}"
            )


@pytest.fixture
def shutdown_serve():
    yield
    serve.shutdown()


def test_usage_lib_cluster_metadata_generation(shutdown_only):
    ray.init(num_cpus=0)
    """
    Test metadata stored is equivalent to `_generate_cluster_metadata`.
    """
    meta = ray_usage_lib._generate_cluster_metadata()
    cluster_metadata = ray_usage_lib.get_cluster_metadata(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
    )
    # Session id is random.
    meta.pop("session_id")
    cluster_metadata.pop("session_id")
    assert meta == cluster_metadata

    """
    Make sure put & get works properly.
    """
    cluster_metadata = ray_usage_lib.put_cluster_metadata(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
    )
    assert cluster_metadata == ray_usage_lib.get_cluster_metadata(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
    )


def test_usage_lib_report_data(shutdown_only, shutdown_serve):
    ray.init(num_cpus=0)
    """
    Make sure the generated data is following the schema.
    """
    cluster_metadata = ray_usage_lib.get_cluster_metadata(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
    )
    d = ray_usage_lib.generate_report_data(cluster_metadata)
    _validate_schema(d)

    """
    Make sure writing to a file works as expected
    """
    global_node = ray.worker._global_node
    temp_dir = global_node.get_temp_dir_path()
    ray_usage_lib.write_usage_data(d, temp_dir)

    def file_exists():
        for path in pathlib.Path(temp_dir).iterdir():
            if usage_constants.USAGE_STATS_FILE in str(path):
                return True
        return False

    wait_for_condition(file_exists)

    """
    Make sure report usage data works as expected
    """
    # Start the ray serve server to verify requests are sent
    # to the right place.
    serve.start()

    @serve.deployment(ray_actor_options={"num_cpus": 0})
    async def usage(request):
        body = await request.json()
        if body == d:
            return True
        else:
            return False

    usage.deploy()

    # Query our endpoint over HTTP.
    r = ray_usage_lib.report_usage_data("http://127.0.0.1:8000/usage", d)
    r.raise_for_status()
    assert json.loads(r.text) is True


@pytest.mark.parametrize(
    "set_env_var",
    [
        {
            "RAY_USAGE_REPORT_URL": "http://127.0.0.1:8000/usage",
            "RAY_USAGE_STATS_ENABLE": "1",
            "RAY_USAGE_REPORT_INTERVAL_S": "1",
        }
    ],
    indirect=True,
)
def test_usage_report_e2e(set_env_var, shutdown_only, shutdown_serve):
    """
    Test usage report works e2e with env vars.
    """
    ray.init(num_cpus=0)

    @ray.remote(num_cpus=0)
    class StatusReporter:
        def __init__(self):
            self.reported = 0
            self.payload = None

        def report_payload(self, payload):
            self.payload = payload

        def reported(self):
            self.reported += 1

        def get(self):
            return self.reported

        def get_payload(self):
            return self.payload

    reporter = StatusReporter.remote()

    serve.start()

    # Usage report should be sent to the URL every 1 second.
    @serve.deployment(ray_actor_options={"num_cpus": 0})
    async def usage(request):
        body = await request.json()
        reporter.reported.remote()
        reporter.report_payload.remote(body)
        return True

    usage.deploy()
    # Since the interval is 1 second, there must have been
    # around 5 requests sent within 10 seconds.
    wait_for_condition(lambda: ray.get(reporter.get.remote()) > 5, timeout=10)
    _validate_schema(ray.get(reporter.get_payload.remote()))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

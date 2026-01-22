import glob
import json
import os
import shutil
import sys

import pytest
from opentelemetry import trace

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve.schema import ReplicaState
from ray.util.tracing.setup_local_tmp_tracing import spans_dir

setup_tracing_path = "ray.util.tracing.setup_local_tmp_tracing:setup_tracing"


@pytest.fixture
def cleanup_spans():
    """Cleanup temporary spans_dir folder at beginning and end of test."""
    if os.path.exists(spans_dir):
        shutil.rmtree(spans_dir)
    os.makedirs(spans_dir, exist_ok=True)
    yield
    # Enable tracing only sets up tracing once per driver process.
    # We set ray.__traced__ to False here so that each
    # test will re-set up tracing.
    ray.__traced__ = False
    if os.path.exists(spans_dir):
        shutil.rmtree(spans_dir)


@pytest.fixture
def ray_serve_with_tracing(cleanup_spans):
    """Start Ray with tracing enabled and Serve."""
    ray.shutdown()
    ray.init(_tracing_startup_hook=setup_tracing_path)
    yield
    serve.shutdown()
    # Shutdown the tracer provider to close file handles before cleanup_spans
    # tries to delete the spans directory.
    tracer_provider = trace.get_tracer_provider()
    if hasattr(tracer_provider, "shutdown"):
        tracer_provider.shutdown()
    ray.shutdown()


def get_span_list():
    """Read span files and return list of span objects."""
    span_list = []
    for entry in glob.glob(f"{spans_dir}**/*.txt", recursive=True):
        with open(entry) as f:
            for line in f.readlines():
                try:
                    span_list.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return span_list


@pytest.mark.skipif(
    sys.platform == "win32", reason="Temp directory cleanup fails on Windows"
)
@pytest.mark.skipif(
    os.environ.get("RAY_SERVE_USE_GRPC_BY_DEFAULT", "0") == "1",
    reason="Tracing context propagation not yet supported in gRPC mode. "
    "See https://github.com/ray-project/ray/issues/60223",
)
def test_deployment_remote_calls_with_tracing(ray_serve_with_tracing):
    serve.start()

    # Create a deployment with custom methods
    @serve.deployment
    class TracedDeployment:
        def __init__(self):
            self.counter = 0

        def get_value(self):
            _ray_trace_ctx = serve.context._get_serve_request_context()._ray_trace_ctx
            assert _ray_trace_ctx is not None
            return 42

        def increment(self):
            _ray_trace_ctx = serve.context._get_serve_request_context()._ray_trace_ctx
            assert _ray_trace_ctx is not None
            self.counter += 1
            return self.counter

    # Deploy the application
    handle = serve.run(TracedDeployment.bind(), name="traced_app")

    # Wait for deployment to be ready
    def check_deployment_ready():
        status = serve.status()
        assert "traced_app" in status.applications

        app_status = status.applications["traced_app"]
        deployment_status = list(app_status.deployments.values())[0]
        num_running = deployment_status.replica_states.get(ReplicaState.RUNNING, 0)
        assert num_running == 1
        return True

    wait_for_condition(check_deployment_ready, timeout=15)

    # Make remote calls to the deployment methods
    # These should work without TypeError about _ray_trace_ctx
    result1 = handle.get_value.remote().result()
    assert result1 == 42

    result2 = handle.increment.remote().result()
    assert result2 == 1

    result3 = handle.increment.remote().result()
    assert result3 == 2

    # Verify that spans were generated for the calls
    def check_spans_generated():
        spans = get_span_list()
        assert len(spans) > 0, "No spans were generated"

        # ServeReplica actor spans should exist
        replica_spans = [s for s in spans if "ServeReplica" in s.get("name", "")]
        assert (
            len(replica_spans) > 0
        ), f"No ServeReplica spans found. Generated {len(spans)} total spans"

        return True

    wait_for_condition(check_spans_generated, timeout=10, retry_interval_ms=500)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))

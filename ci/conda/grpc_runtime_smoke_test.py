"""Smoke test for a Ray wheel running next to conda-forge's shared gRPC.

Run by ci/conda/grpc_runtime_test.sh inside a conda-forge environment. See
that script for why this combination needs its own test.

Each scenario runs in its own subprocess with a timeout, because the failure
this guards against is usually a hang, not an exception.
"""

import argparse
import os
import socket
import subprocess
import sys
import time
from importlib import metadata

SCENARIO_TIMEOUT_S = 300


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _loaded_libraries() -> set:
    paths = set()
    with open("/proc/self/maps") as f:
        for line in f:
            parts = line.split(maxsplit=5)
            if len(parts) == 6 and parts[5].startswith("/"):
                paths.add(parts[5].strip())
    return paths


def check_environment(args) -> None:
    """Fail unless conda's shared gRPC and Ray's static gRPC share this process."""
    prefix = os.path.realpath(sys.prefix)

    for name in ("grpcio", "protobuf"):
        installer = (metadata.distribution(name).read_text("INSTALLER") or "").strip()
        assert installer == "conda", (
            f"{name} {metadata.version(name)} was installed by {installer!r}, not "
            "conda. pip replaced the conda package, so this run would not test "
            "conda's gRPC."
        )

    import grpc

    import ray
    import ray._raylet

    loaded = _loaded_libraries()
    libgrpc = sorted(p for p in loaded if os.path.basename(p).startswith("libgrpc.so"))
    assert libgrpc, "grpcio did not load a shared libgrpc.so; is it the conda build?"
    for path in libgrpc:
        assert os.path.realpath(path).startswith(
            prefix
        ), f"libgrpc was loaded from outside the conda env: {path}"
    assert os.path.realpath(ray._raylet.__file__) in {
        os.path.realpath(p) for p in loaded
    }, "_raylet.so is not loaded"

    print(f"ray {ray.__version__} ({ray.__commit__})")
    print(f"grpcio {grpc.__version__} (conda)")
    print(f"protobuf {metadata.version('protobuf')} (conda)")
    for path in libgrpc:
        print(f"shared gRPC: {path}")


def check_ordered_actor_calls(args) -> None:
    """Actor calls from one caller arrive in submission order."""
    import ray

    ray.init(address=args.gcs_address)

    @ray.remote
    class Recorder:
        def __init__(self):
            self.seen = []

        def record(self, i):
            self.seen.append(i)

        def get(self):
            return self.seen

    n = 200
    actors = [Recorder.remote() for _ in range(4)]
    for i in range(n):
        for actor in actors:
            actor.record.remote(i)
    for seen in ray.get([actor.get.remote() for actor in actors]):
        assert seen == list(range(n)), f"calls arrived out of order: {seen[:20]}..."


def check_ray_client(args) -> None:
    """A Ray Client driver can connect, and run tasks and actors."""
    import ray

    ray.init(address=f"ray://127.0.0.1:{args.client_port}")

    @ray.remote
    def square(x):
        return x * x

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    assert ray.get([square.remote(i) for i in range(20)]) == [i * i for i in range(20)]
    counter = Counter.remote()
    assert ray.get([counter.increment.remote() for _ in range(20)]) == list(
        range(1, 21)
    )
    ray.shutdown()


def check_serve_grpc(args) -> None:
    """A Serve gRPC ingress answers a request made with conda's grpcio."""
    import grpc

    import ray
    from ray import serve
    from ray.serve.config import gRPCOptions
    from ray.serve.generated import serve_pb2, serve_pb2_grpc

    ray.init(address=args.gcs_address)
    serve.start(
        http_options={"port": args.serve_http_port},
        grpc_options=gRPCOptions(
            port=args.serve_grpc_port,
            grpc_servicer_functions=[
                "ray.serve.generated.serve_pb2_grpc."
                "add_UserDefinedServiceServicer_to_server"
            ],
        ),
    )

    @serve.deployment
    class Greeter:
        def __call__(self, request: serve_pb2.UserDefinedMessage):
            return serve_pb2.UserDefinedResponse(
                greeting=f"Hello {request.name}", num_x2=request.num * 2
            )

    serve.run(Greeter.bind())

    with grpc.insecure_channel(f"127.0.0.1:{args.serve_grpc_port}") as channel:
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        for i in range(20):
            response = stub.__call__(
                serve_pb2.UserDefinedMessage(name="conda", num=i), timeout=60
            )
            assert response.greeting == "Hello conda", response
            assert response.num_x2 == i * 2, response
    serve.shutdown()


def check_dashboard(args) -> None:
    """The state API and a job submission work through the dashboard and agent."""
    import ray
    from ray.job_submission import JobStatus, JobSubmissionClient
    from ray.util.state import list_actors, list_nodes

    ray.init(address=args.gcs_address)

    @ray.remote
    class Marker:
        def ping(self):
            return "pong"

    marker = Marker.remote()
    assert ray.get(marker.ping.remote()) == "pong"

    dashboard = f"http://127.0.0.1:{args.dashboard_port}"
    assert len(list_nodes(address=dashboard)) == 1
    # A class defined inside a function is listed under its qualified name.
    class_names = [a.class_name for a in list_actors(address=dashboard)]
    assert any(name.endswith("Marker") for name in class_names), class_names

    client = JobSubmissionClient(dashboard)
    job_id = client.submit_job(
        entrypoint="python -c \"import ray; ray.init(); print('job ok')\""
    )
    deadline = time.time() + 180
    status = client.get_job_status(job_id)
    while not status.is_terminal() and time.time() < deadline:
        time.sleep(1)
        status = client.get_job_status(job_id)
    assert (
        status == JobStatus.SUCCEEDED
    ), f"job ended as {status}:\n{client.get_job_logs(job_id)}"


SCENARIOS = {
    "environment": check_environment,
    "ordered_actor_calls": check_ordered_actor_calls,
    "ray_client": check_ray_client,
    "serve_grpc": check_serve_grpc,
    "dashboard": check_dashboard,
}


def _parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--gcs-address")
    for name in ("client-port", "dashboard-port", "serve-http-port", "serve-grpc-port"):
        parser.add_argument(f"--{name}", type=int)
    return parser.parse_args()


def _ray(*cmd, check=True):
    # Use the ray CLI that belongs to this interpreter, whatever PATH says.
    return subprocess.run(
        [sys.executable, "-m", "ray.scripts.scripts", *cmd], check=check
    )


def main() -> int:
    args = _parse_args()
    if args.scenario:
        SCENARIOS[args.scenario](args)
        return 0

    gcs_port = _free_port()
    ports = {
        "--client-port": _free_port(),
        "--dashboard-port": _free_port(),
        "--serve-http-port": _free_port(),
        "--serve-grpc-port": _free_port(),
    }
    child_args = ["--gcs-address", f"127.0.0.1:{gcs_port}"]
    for flag, port in ports.items():
        child_args += [flag, str(port)]

    failed = []
    _ray(
        "start",
        "--head",
        f"--port={gcs_port}",
        f"--ray-client-server-port={ports['--client-port']}",
        f"--dashboard-port={ports['--dashboard-port']}",
        "--num-cpus=4",
    )
    try:
        for name in SCENARIOS:
            print(f"\n=== {name} ===", flush=True)
            start = time.time()
            try:
                subprocess.run(
                    [sys.executable, __file__, "--scenario", name, *child_args],
                    check=True,
                    timeout=SCENARIO_TIMEOUT_S,
                )
                outcome = "PASSED"
            except subprocess.TimeoutExpired:
                outcome = f"TIMED OUT after {SCENARIO_TIMEOUT_S}s"
                failed.append(name)
            except subprocess.CalledProcessError as e:
                outcome = f"FAILED (exit code {e.returncode})"
                failed.append(name)
            print(
                f"=== {name}: {outcome} in {time.time() - start:.0f}s ===", flush=True
            )
    finally:
        _ray("stop", "--force", check=False)

    if failed:
        print(f"\nFailed scenarios: {', '.join(failed)}")
        return 1
    print(f"\nAll {len(SCENARIOS)} scenarios passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Checks that a RayService keeps serving while its Ray head and worker Pods die.

Applies serve_ha.yaml: Redis for GCS fault tolerance, a RayService that runs
serve_ha_app.py on a head and three workers, and a client Pod that sends the
app a steady stream of requests. Then deletes worker and head Pods one at a
time, waits for the RayService to fully recover from each deletion, and checks
what the client saw during every window.

Run it with kubectl pointing at an empty default namespace of a Kubernetes
cluster that has the KubeRay operator and the ray-ci:kuberay-test image.
ci/k8s/run-serve-ha-test.sh sets one up in kind. It deletes everything it
deployed when it exits.
"""

import collections
import json
import pathlib
import random
import secrets
import subprocess
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

HERE = pathlib.Path(__file__).parent
NAME = "serve-ha"
CLIENT = "serve-ha-client"
# Prints Serve's and Ray's view of the cluster.
CLUSTER_DETAILS = [
    "exec",
    CLIENT,
    "--",
    "python",
    "/home/ray/serve_ha/serve_ha_client.py",
    "details",
]
APP = "ha"
# Alternates, so each recovery must leave the cluster able to survive the other
# kind of failure.
KILLS = ["worker", "head", "worker", "head", "worker"]
RECOVERY_TIMEOUT_S = 600
# Longer than the client's slowest request, 3 attempts with 5s timeouts, so the
# requests that a deletion fails finish inside its window.
SETTLE_S = 30
STEADY_S = 60
# No window may go longer than this without a successful request. With 3 CPU
# stressors on 4 cores, head Pod deletions caused gaps of up to 5s.
MAX_OUTAGE_S = 10


def log(message: str):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def kubectl(*args: str, stdin: Optional[str] = None, timeout_s: int = 60) -> str:
    command = f"kubectl {' '.join(args)}"
    try:
        result = subprocess.run(
            ["kubectl", *args],
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"`{command}` timed out.")
    if result.returncode != 0:
        raise RuntimeError(f"`{command}` failed: {result.stderr.strip()}")
    return result.stdout


def get(*args: str) -> dict:
    return json.loads(kubectl("get", *args, "-o", "json"))


def deploy():
    objects = [
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": NAME},
            # KubeRay reads the Ray auth token from the auth_token key.
            "stringData": {
                "auth_token": secrets.token_hex(32),
                "redis_password": secrets.token_hex(16),
            },
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": f"{NAME}-scripts"},
            "data": {
                name: (HERE / name).read_text()
                for name in ["serve_ha_app.py", "serve_ha_client.py"]
            },
        },
    ]
    kubectl(
        "apply",
        "-f",
        "-",
        stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}),
    )
    kubectl("apply", "-f", str(HERE / "serve_ha.yaml"))


@dataclass
class Snapshot:
    rayservice: dict
    head: List[dict]
    workers: List[dict]
    # The Serve REST API's response under "serve" and Ray's nodes under "nodes",
    # or None if the Ray dashboard is unreachable.
    details: Optional[dict]

    @property
    def cluster(self) -> Optional[str]:
        status = self.rayservice.get("status", {})
        return status.get("activeServiceStatus", {}).get("rayClusterName")

    def replicas(self) -> List[dict]:
        deployments = self.details["serve"]["applications"][APP]["deployments"]
        return [replica for d in deployments.values() for replica in d["replicas"]]


def take_snapshot() -> Snapshot:
    pods = get("pods", "-l", "ray.io/node-type")["items"]
    try:
        details = json.loads(kubectl(*CLUSTER_DETAILS))
    except RuntimeError:
        details = None
    return Snapshot(
        rayservice=get("rayservice", NAME),
        head=[p for p in pods if p["metadata"]["labels"]["ray.io/node-type"] == "head"],
        workers=[
            p for p in pods if p["metadata"]["labels"]["ray.io/node-type"] == "worker"
        ],
        details=details,
    )


def is_running(pod: dict) -> bool:
    return (
        "deletionTimestamp" not in pod["metadata"]
        and pod["status"].get("phase") == "Running"
    )


def is_ready(pod: dict) -> bool:
    conditions = pod["status"].get("conditions", [])
    return is_running(pod) and any(
        c["type"] == "Ready" and c["status"] == "True" for c in conditions
    )


def find_problem(
    snapshot: Snapshot, cluster: Optional[str], dead_replicas: Set[str]
) -> Optional[str]:
    """Returns why the RayService isn't fully healthy, or None if it is."""
    status = snapshot.rayservice.get("status", {})
    ready = [c for c in status.get("conditions", []) if c["type"] == "Ready"]
    if not ready or ready[0]["status"] != "True":
        return f"the RayService isn't Ready: {ready[0]['message'] if ready else ''}"
    if cluster is not None and snapshot.cluster != cluster:
        return f"the RayService moved to RayCluster {snapshot.cluster}"
    if status.get("pendingServiceStatus", {}).get("rayClusterName"):
        return "the RayService is creating a new RayCluster"
    if len(snapshot.head) != 1 or not is_ready(snapshot.head[0]):
        return "the head Pod isn't ready"
    spec = snapshot.rayservice["spec"]["rayClusterConfig"]
    num_workers = spec["workerGroupSpecs"][0]["replicas"]
    running = sum(map(is_running, snapshot.workers))
    if len(snapshot.workers) != num_workers or running != num_workers:
        return (
            f"{running} of {len(snapshot.workers)} worker Pods are running, "
            f"expected {num_workers}"
        )
    if snapshot.details is None:
        return "the Ray dashboard is unreachable"
    alive = sorted(
        node["node_ip"]
        for node in snapshot.details["nodes"]
        if node["state"] == "ALIVE"
    )
    pod_ips = sorted(pod["status"]["podIP"] for pod in snapshot.head + snapshot.workers)
    if alive != pod_ips:
        return f"Ray's alive nodes {alive} don't match the Pods {pod_ips}"
    app = snapshot.details["serve"].get("applications", {}).get(APP)
    if app is None or app["status"] != "RUNNING":
        return f"the Serve app is {app['status'] if app else 'missing'}"
    for deployment in app["deployments"].values():
        states = [replica["state"] for replica in deployment["replicas"]]
        expected = ["RUNNING"] * deployment["target_num_replicas"]
        if deployment["status"] != "HEALTHY" or states != expected:
            return (
                f"deployment {deployment['name']} is {deployment['status']} with "
                f"replicas {states}"
            )
    if dead_replicas & {replica["actor_id"] for replica in snapshot.replicas()}:
        return "Serve still lists replicas from the deleted Pod"
    # KubeRay's readiness probe for RayService workers also checks the Serve proxy,
    # which Serve drains on nodes without replicas.
    replica_ips = {replica["node_ip"] for replica in snapshot.replicas()}
    unready = [
        pod["metadata"]["name"]
        for pod in snapshot.workers
        if pod["status"]["podIP"] in replica_ips and not is_ready(pod)
    ]
    if unready:
        return f"worker Pods {unready} host replicas but aren't ready"
    return None


def wait_for_recovery(
    cluster: Optional[str], dead_replicas: Set[str] = frozenset()
) -> Snapshot:
    """Waits until the RayService is fully healthy in 3 checks in a row."""
    start = time.monotonic()
    healthy_checks = 0
    last_problem = None
    while healthy_checks < 3:
        if time.monotonic() - start > RECOVERY_TIMEOUT_S:
            raise TimeoutError(
                f"The RayService didn't recover in {RECOVERY_TIMEOUT_S}s: "
                f"{last_problem}."
            )
        time.sleep(2)
        try:
            snapshot = take_snapshot()
            problem = find_problem(snapshot, cluster, dead_replicas)
        except RuntimeError as e:
            problem = str(e)
        if problem is None:
            healthy_checks += 1
            continue
        healthy_checks = 0
        if problem != last_problem:
            log(f"Waiting: {problem}.")
            last_problem = problem
    return snapshot


def delete_pod(pod: dict, description: str):
    log(f"Deleting {description} {pod['metadata']['name']}.")
    kubectl("delete", "pod", pod["metadata"]["name"], "--wait=false")


def delete_worker(snapshot: Snapshot) -> Set[str]:
    """Deletes a worker Pod and returns the actor IDs of the replicas on it."""
    replicas_by_ip = collections.defaultdict(set)
    for replica in snapshot.replicas():
        replicas_by_ip[replica["node_ip"]].add(replica["actor_id"])
    candidates = [p for p in snapshot.workers if replicas_by_ip[p["status"]["podIP"]]]
    if not candidates:
        raise RuntimeError(f"No worker Pod hosts a replica: {dict(replicas_by_ip)}")
    pod = random.choice(candidates)
    replicas = replicas_by_ip[pod["status"]["podIP"]]
    delete_pod(pod, f"the worker Pod with {len(replicas)} replicas,")
    return replicas


def worker_restarts(snapshot: Snapshot) -> Dict[str, int]:
    return {
        pod["metadata"]["name"]: sum(
            c["restartCount"] for c in pod["status"]["containerStatuses"]
        )
        for pod in snapshot.workers
    }


def check_head_recovery(before: Snapshot, after: Snapshot):
    """Checks that the worker Pods and replicas outlived the head Pod."""
    if worker_restarts(before) != worker_restarts(after):
        raise AssertionError(
            "The worker Pods didn't survive the head Pod's deletion. Restarts went "
            f"from {worker_restarts(before)} to {worker_restarts(after)}."
        )
    replicas_before = {replica["actor_id"] for replica in before.replicas()}
    replicas_after = {replica["actor_id"] for replica in after.replicas()}
    if replicas_before != replicas_after:
        raise AssertionError(
            f"Serve replaced replicas {sorted(replicas_before - replicas_after)} "
            "after the head Pod's deletion."
        )


@dataclass
class Window:
    name: str
    start: float
    end: float


def steady_window(name: str) -> Window:
    start = time.time()
    time.sleep(STEADY_S)
    return Window(name, start, time.time())


def run() -> List[Window]:
    deploy()
    cluster = wait_for_recovery(cluster=None).cluster
    log(f"The RayService is healthy on RayCluster {cluster}.")
    # Lets requests sent before the app was up finish before the first window.
    time.sleep(SETTLE_S)
    windows = [steady_window("steady start")]
    for i, kill in enumerate(KILLS, 1):
        start = time.time()
        before = wait_for_recovery(cluster)
        if kill == "worker":
            wait_for_recovery(cluster, delete_worker(before))
        else:
            delete_pod(before.head[0], "the head Pod")
            check_head_recovery(before, wait_for_recovery(cluster))
        log(f"Recovered in {time.time() - start:.0f}s.")
        time.sleep(SETTLE_S)
        windows.append(Window(f"{kill} kill {i}", start, time.time()))
    windows.append(steady_window("steady end"))
    return windows


def check(windows: List[Window]):
    # Lets the client report the last second of the last window.
    time.sleep(2)
    counts = collections.defaultdict(collections.Counter)
    errors = []
    for line in kubectl("logs", CLIENT).splitlines():
        if not line.startswith("{"):
            errors.append(line)
            continue
        record = json.loads(line)
        t = record.pop("t")
        if "error" in record:
            errors.append(
                f"[{time.strftime('%H:%M:%S', time.localtime(t))}] {record['error']}"
            )
        else:
            counts[t].update(record)
    failures = []
    log("window          seconds      ok  retried  failed  wrong  longest outage (s)")
    for window in windows:
        total = collections.Counter()
        outage_s = longest_outage_s = 0
        seconds = range(int(window.start), int(window.end))
        for t in seconds:
            total.update(counts[t])
            outage_s = 0 if counts[t]["ok"] else outage_s + 1
            longest_outage_s = max(longest_outage_s, outage_s)
        log(
            f"{window.name:<14} {len(seconds):>8} {total['ok']:>7} "
            f"{total['retried']:>8} {total['failed']:>7} {total['wrong']:>6} "
            f"{longest_outage_s:>19}"
        )
        if total["wrong"]:
            failures.append(f"{window.name}: {total['wrong']} wrong responses")
        if total["failed"]:
            failures.append(
                f"{window.name}: {total['failed']} requests failed every attempt"
            )
        if longest_outage_s > MAX_OUTAGE_S:
            failures.append(
                f"{window.name}: no request succeeded for {longest_outage_s}s, "
                f"more than {MAX_OUTAGE_S}s"
            )
    if failures:
        print("\n".join(["Client errors:", *errors[-200:]]), flush=True)
        raise AssertionError("\n".join(failures))


def dump_diagnostics():
    commands = [
        ["get", "pods", "-o", "wide"],
        ["get", "rayservice", NAME, "-o", "yaml"],
        ["get", "events", "--sort-by=.lastTimestamp"],
        ["logs", "-n", "kuberay-system", "deployment/kuberay-operator", "--tail=300"],
        CLUSTER_DETAILS,
    ]
    try:
        heads = get("pods", "-l", "ray.io/node-type=head")["items"]
    except RuntimeError:
        heads = []
    for head in heads:
        commands.append(
            [
                "exec",
                head["metadata"]["name"],
                "--",
                "sh",
                "-c",
                "tail -n 300 /tmp/ray/session_latest/logs/serve/controller_*.log",
            ]
        )
    for args in commands:
        print(f"--- kubectl {' '.join(args)}", flush=True)
        try:
            print(kubectl(*args), flush=True)
        except RuntimeError as e:
            print(e, flush=True)


def cleanup():
    """Deletes everything deploy() created.

    Each delete runs in the foreground, so it returns only once what the object
    owns, such as the RayService's RayCluster and Redis' Pod, is gone too. The
    RayService goes first: KubeRay keeps its RayCluster until a Job has deleted
    its GCS state from Redis, and that Job needs Redis and the Secret.
    """
    log("Deleting what the test deployed.")
    for args in [
        ["pod/" + CLIENT, "rayservice/" + NAME],
        ["-f", str(HERE / "serve_ha.yaml")],
        ["secret/" + NAME, f"configmap/{NAME}-scripts"],
    ]:
        try:
            kubectl(
                "delete",
                *args,
                "--cascade=foreground",
                "--ignore-not-found",
                "--timeout=5m",
                timeout_s=330,
            )
        except RuntimeError as e:
            print(e, flush=True)


def main():
    try:
        check(run())
    except BaseException:
        dump_diagnostics()
        print("+++ serve_ha.py failed", flush=True)
        raise
    finally:
        cleanup()


if __name__ == "__main__":
    main()

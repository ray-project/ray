import enum
import json
import subprocess
from kubernetes import client, config, watch
import requests
import random
import uuid
import pathlib
import time
import ray
import os

# global variables for the cluster informations
CLUSTER_ID = None
RAY_CLUSTER_NAME = None
RAY_SERVICE_NAME = None
LOCUST_ID = None


# Kill node type
class TestScenario(enum.Enum):
    KILL_WORKER_NODE = "kill_worker_node"
    KILL_HEAD_NODE = "kill_head_node"


if os.environ.get("RAY_IMAGE") is not None:
    ray_image = os.environ.get("RAY_IMAGE")
elif ray.__version__ != "3.0.0.dev0":
    ray_image = f"rayproject/ray:{ray.__version__}"
elif ray.__commit__ == "{{RAY_COMMIT_SHA}}":
    ray_image = "rayproject/ray:nightly"
else:
    ray_image = f"rayproject/ray:{ray.__commit__[:6]}"

config.load_kube_config()
cli = client.CoreV1Api()

yaml_path = pathlib.Path("/tmp/ray_v1alpha1_rayservice.yaml")


def generate_cluster_variable():
    global CLUSTER_ID
    global RAY_CLUSTER_NAME
    global RAY_SERVICE_NAME
    global LOCUST_ID
    CLUSTER_ID = str(uuid.uuid4()).split("-")[0]
    RAY_CLUSTER_NAME = "cluster-" + CLUSTER_ID
    RAY_SERVICE_NAME = "service-" + CLUSTER_ID
    LOCUST_ID = "ray-locust-" + CLUSTER_ID


def check_kuberay_installed():
    # Make sure the ray namespace exists
    KUBERAY_VERSION = "v0.3.0"
    uri = (
        "github.com/ray-project/kuberay/manifests"
        f"/base?ref={KUBERAY_VERSION}&timeout=90s"
    )
    print(
        subprocess.check_output(
            [
                "kubectl",
                "apply",
                "-k",
                uri,
            ]
        ).decode()
    )
    pods = subprocess.check_output(
        ["kubectl", "get", "pods", "--namespace", "ray-system", "--no-headers"]
    ).decode()
    assert pods.split("\n") != 0


def start_rayservice():
    # step-1: generate the yaml file
    print(f"Using ray image: {ray_image}")
    solution = "\n".join(
        [
            f"    {line}"
            for line in pathlib.Path("./solution.py").read_text().splitlines()
        ]
    )
    locustfile = "\n".join(
        [
            f"    {line}"
            for line in pathlib.Path("./locustfile.py").read_text().splitlines()
        ]
    )
    template = (
        pathlib.Path("ray_v1alpha1_rayservice_template.yaml")
        .read_text()
        .format(
            cluster_id=CLUSTER_ID,
            ray_image=ray_image,
            solution=solution,
            locustfile=locustfile,
        )
    )

    print("=== YamlFile ===")
    print(template)
    tmp_yaml = pathlib.Path("/tmp/ray_v1alpha1_rayservice.yaml")
    tmp_yaml.write_text(template)

    print("=== Get Pods from ray-system ===")
    print(
        subprocess.check_output(
            ["kubectl", "get", "pods", "--namespace", "ray-system", "--no-headers"]
        ).decode()
    )

    # step-2: create the cluter
    print(f"Creating cluster with id: {CLUSTER_ID}")
    print(subprocess.check_output(["kubectl", "create", "-f", str(tmp_yaml)]).decode())

    # step-3: make sure the ray cluster is up
    w = watch.Watch()
    start_time = time.time()
    head_pod_name = None
    for event in w.stream(
        func=cli.list_namespaced_pod,
        namespace="default",
        label_selector=f"rayCluster={RAY_CLUSTER_NAME},ray.io/node-type=head",
        timeout_seconds=60,
    ):
        if event["object"].status.phase == "Running":
            assert event["object"].kind == "Pod"
            head_pod_name = event["object"].metadata.name
            end_time = time.time()
            print(f"{CLUSTER_ID} started in {end_time-start_time} sec")
            print(f"head pod {head_pod_name}")
            break
    assert head_pod_name is not None
    # step-4: e2e check it's alive
    cmd = """
import requests
print(requests.get('http://localhost:8000/?val=123').text)
"""
    while True:
        try:
            resp = (
                subprocess.check_output(
                    f'kubectl exec {head_pod_name} -- python -c "{cmd}"', shell=True
                )
                .decode()
                .strip()
            )
            if resp == "375":
                print("Service is up now!")
                break
            else:
                print(f"Failed with msg {resp}")
        except Exception as e:
            print("Error", e)
        time.sleep(2)


def start_port_forward():
    proc = None
    proc = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            f"svc/{RAY_SERVICE_NAME}-serve-svc",
            "8000:8000",
            "--address=0.0.0.0",
        ]
    )

    while True:
        try:
            resp = requests.get(
                "http://localhost:8000/",
                timeout=1,
                params={
                    "val": 10,
                },
            )
            if resp.status_code == 200:
                print("The ray service is ready!!!")
                break
        except requests.exceptions.Timeout:
            pass
        except requests.exceptions.ConnectionError:
            pass

        print("Waiting for the proxy to be alive")
        time.sleep(1)

    return proc


def warmup_cluster(num_reqs):
    for _ in range(num_reqs):
        resp = requests.get(
            "http://localhost:8000/",
            timeout=1,
            params={
                "val": 10,
            },
        )
        assert resp.status_code == 200


def start_sending_traffics(duration, users):
    print("=== Install locust by helm ===")
    yaml_config = (
        pathlib.Path("locust-run.yaml")
        .read_text()
        .format(users=users, cluster_id=CLUSTER_ID, duration=int(duration))
    )
    print("=== Locust YAML ===")
    print(yaml_config)

    pathlib.Path("/tmp/locust-run-config.yaml").write_text(yaml_config)
    helm_install_logs = subprocess.check_output(
        [
            "helm",
            "install",
            LOCUST_ID,
            "deliveryhero/locust",
            "-f",
            "/tmp/locust-run-config.yaml",
        ]
    )
    print(helm_install_logs)

    timeout_wait_for_locust_s = 300
    while timeout_wait_for_locust_s > 0:
        labels = [
            f"app.kubernetes.io/instance=ray-locust-{CLUSTER_ID}",
            "app.kubernetes.io/name=locust,component=master",
        ]
        pods = cli.list_namespaced_pod("default", label_selector=",".join(labels))
        assert len(pods.items) == 1

        if pods.items[0].status.phase == "Pending":
            print("Waiting for the locust pod to be ready...")
            time.sleep(30)
            timeout_wait_for_locust_s -= 30
        else:
            break

    proc = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            f"svc/ray-locust-{CLUSTER_ID}",
            "8080:8089",
            "--address=0.0.0.0",
        ]
    )
    return proc


def dump_pods_actors(pod_name):
    print(
        subprocess.run(
            f"kubectl exec {pod_name} -- ps -ef | grep ::",
            shell=True,
            capture_output=True,
        ).stdout.decode()
    )


def kill_head():
    pods = cli.list_namespaced_pod(
        "default",
        label_selector=f"rayCluster={RAY_CLUSTER_NAME},ray.io/node-type=head",
    )
    if pods.items[0].status.phase == "Running":
        print(f"Killing header {pods.items[0].metadata.name}")
        dump_pods_actors(pods.items[0].metadata.name)
        cli.delete_namespaced_pod(pods.items[0].metadata.name, "default")


def kill_worker():
    pods = cli.list_namespaced_pod(
        "default",
        label_selector=f"rayCluster={RAY_CLUSTER_NAME},ray.io/node-type=worker",
    )
    alive_pods = [
        (p.status.start_time, p.metadata.name)
        for p in pods.items
        if p.status.phase == "Running"
    ]
    # sorted(alive_pods)
    # We kill the oldest nodes for now given the memory leak in serve.
    # to_be_killed = alive_pods[-1][1]

    to_be_killed = random.choice(alive_pods)[1]
    print(f"Killing worker {to_be_killed}")
    dump_pods_actors(pods.items[0].metadata.name)
    cli.delete_namespaced_pod(to_be_killed, "default")


def start_killing_nodes(duration, kill_interval, kill_node_type):
    """Kill the nodes in ray cluster.

    duration: How long does we run the test (seconds)
    kill_interval: The interval between two kills (seconds)
    kill_head_every_n: For every n kills, we kill a head node
    kill_node_type: kill either worker node or head node
    """

    for kill_idx in range(1, int(duration / kill_interval)):
        while True:
            try:
                # kill
                if kill_node_type == TestScenario.KILL_HEAD_NODE:
                    kill_head()
                elif kill_node_type == TestScenario.KILL_WORKER_NODE:
                    kill_worker()
                break
            except Exception as e:
                from time import sleep

                print(f"Fail to kill node, retry in 5 seconds: {e}")
                sleep(5)

        time.sleep(kill_interval)


def get_stats():
    labels = [
        f"app.kubernetes.io/instance=ray-locust-{CLUSTER_ID}",
        "app.kubernetes.io/name=locust,component=master",
    ]
    pods = cli.list_namespaced_pod("default", label_selector=",".join(labels))
    assert len(pods.items) == 1
    pod_name = pods.items[0].metadata.name
    subprocess.check_output(
        [
            "kubectl",
            "cp",
            f"{pod_name}:/home/locust/test_result_{CLUSTER_ID}_stats_history.csv",
            "./stats_history.csv",
        ]
    )
    data = []
    with open("stats_history.csv") as f:
        import csv

        reader = csv.reader(f)
        for d in reader:
            data.append(d)
    # The first 5mins is for warming up
    offset = 300
    start_time = int(data[offset][0])
    end_time = int(data[-1][0])
    # 17 is the index for total requests
    # 18 is the index for total failed requests
    total = float(data[-1][17]) - float(data[offset][17])
    failures = float(data[-1][18]) - float(data[offset][18])

    # Available, through put
    return (total - failures) / total, total / (end_time - start_time), data


def main():
    result = {
        TestScenario.KILL_WORKER_NODE.value: {"rate": None},
        TestScenario.KILL_HEAD_NODE.value: {"rate": None},
    }
    expected_result = {
        TestScenario.KILL_HEAD_NODE: 0.99,
        TestScenario.KILL_HEAD_NODE: 0.99,
    }
    check_kuberay_installed()
    users = 60
    for kill_node_type, kill_interval, test_duration in [
        (TestScenario.KILL_WORKER_NODE, 60, 600),
        (TestScenario.KILL_HEAD_NODE, 300, 1200),
    ]:
        try:
            generate_cluster_variable()
            procs = []
            start_rayservice()
            procs.append(start_port_forward())
            warmup_cluster(200)
            procs.append(start_sending_traffics(test_duration * 1.1, users))
            start_killing_nodes(test_duration, kill_interval, kill_node_type)
            rate, qps, data = get_stats()
            print("Raw Data", data, qps)
            result[kill_node_type.value]["rate"] = rate
            assert expected_result[kill_node_type] <= rate
            assert qps > users * 10 * 0.8
        except Exception as e:
            print(f"{kill_node_type} HA test failed, {e}")
        finally:
            print("=== Cleanup ===")
            subprocess.run(
                ["kubectl", "delete", "-f", str(yaml_path)],
                capture_output=True,
            )
            subprocess.run(
                ["helm", "uninstall", LOCUST_ID],
                capture_output=True,
            )
            for p in procs:
                p.kill()
            print("==== Cleanup done ===")
        print("Result:", result)

        test_output_json_path = os.environ.get(
            "TEST_OUTPUT_JSON", "/tmp/release_test_output.json"
        )
        with open(test_output_json_path, "wt") as f:
            json.dump(result, f)


if __name__ == "__main__":
    try:
        # Connect to ray so that the auto suspense
        # will not start.
        ray.init("auto")
    except Exception:
        # It doesnt' matter if it failed.
        pass
    main()

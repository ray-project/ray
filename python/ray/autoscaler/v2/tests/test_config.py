# coding: utf-8
import importlib
import os
import sys
import tempfile

import pytest  # noqa

from ray._common.utils import binary_to_hex
from ray._private.test_utils import get_test_config_path
from ray.autoscaler import AUTOSCALER_DIR_PATH
from ray.autoscaler._private.constants import PENDING_NODE_IDLE_TERMINATION_GATED_KEY
from ray.autoscaler._private.util import format_readonly_node_type
from ray.autoscaler.v2.instance_manager import config as config_mod
from ray.autoscaler.v2.instance_manager.config import (
    AutoscalingConfig,
    FileConfigReader,
    Provider,
    ReadOnlyProviderConfigReader,
)


@pytest.mark.parametrize(
    "skip_hash",
    [True, False],
)
def test_simple(skip_hash):
    config = FileConfigReader(
        get_test_config_path("test_multi_node.yaml"), skip_content_hash=skip_hash
    ).get_cached_autoscaling_config()
    assert config.get_cloud_node_config("head_node") == {"InstanceType": "m5.large"}
    assert config.get_docker_config("head_node") == {
        "image": "anyscale/ray-ml:latest",
        "container_name": "ray_container",
        "pull_before_run": True,
    }
    assert config.get_worker_start_ray_commands


def test_complex():
    config = FileConfigReader(
        get_test_config_path("test_ray_complex.yaml")
    ).get_cached_autoscaling_config()
    assert config.get_head_setup_commands() == [
        "echo a",
        "echo b",
        "echo ${echo hi}",
        "echo head",
    ]
    assert config.get_head_start_ray_commands() == [
        "ray stop",
        "ray start --head --autoscaling-config=~/ray_bootstrap_config.yaml",
    ]
    assert config.get_worker_setup_commands("worker_nodes") == [
        "echo a",
        "echo b",
        "echo ${echo hi}",
        "echo worker",
    ]
    assert config.get_worker_start_ray_commands() == [
        "ray stop",
        "ray start --address=$RAY_HEAD_IP",
    ]
    assert config.get_worker_setup_commands("worker_nodes1") == [
        "echo worker1",
    ]

    assert config.get_docker_config("head_node") == {
        "image": "anyscale/ray-ml:head-default",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_docker_config("default") == {
        "image": "anyscale/ray-ml:worker-default",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_docker_config("worker_nodes") == {
        "image": "anyscale/ray-ml:worker-default",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_docker_config("worker_nodes1") == {
        "image": "anyscale/ray-ml:worker_nodes1",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_initialization_commands("worker_nodes") == ["echo what"]

    assert config.get_initialization_commands("worker_nodes1") == ["echo init"]

    assert config.get_node_resources("worker_nodes1") == {"CPU": 2}

    assert config.get_node_resources("worker_nodes") == {}

    assert config.get_node_labels("worker_nodes1") == {"foo": "bar"}

    assert config.get_config("cluster_name") == "test-cli"
    assert config.get_config("non-existing", "default") == "default"
    assert config.get_config("non-existing") is None


def test_multi_provider_instance_type():
    def load_config(file):
        path = os.path.join(AUTOSCALER_DIR_PATH, file)
        return FileConfigReader(path).get_cached_autoscaling_config()

    aws_config = load_config("aws/defaults.yaml")
    assert aws_config.get_provider_instance_type("ray.head.default") == "m5.large"

    gcp_config = load_config("gcp/defaults.yaml")
    # NOTE: Why is this underscore....
    assert gcp_config.get_provider_instance_type("ray_head_default") == "n1-standard-2"

    aliyun_config = load_config("aliyun/defaults.yaml")
    assert (
        aliyun_config.get_provider_instance_type("ray.head.default") == "ecs.n4.large"
    )

    azure_config = load_config("azure/defaults.yaml")
    assert (
        azure_config.get_provider_instance_type("ray.head.default") == "Standard_D2s_v3"
    )

    # TODO(rickyx):
    # We don't have kuberay and local config yet.


def test_node_type_configs():
    config = FileConfigReader(
        get_test_config_path("test_ray_complex.yaml")
    ).get_cached_autoscaling_config()

    node_type_configs = config.get_node_type_configs()
    assert config.get_max_num_worker_nodes() == 10
    assert len(node_type_configs) == 4
    assert node_type_configs["head_node"].max_worker_nodes == 1
    assert node_type_configs["head_node"].min_worker_nodes == 0
    assert node_type_configs["head_node"].resources == {}
    assert node_type_configs["head_node"].labels == {}

    assert node_type_configs["default"].max_worker_nodes == 2
    assert node_type_configs["default"].min_worker_nodes == 0
    assert node_type_configs["default"].resources == {}
    assert node_type_configs["default"].labels == {}

    assert node_type_configs["worker_nodes"].max_worker_nodes == 2
    assert node_type_configs["worker_nodes"].min_worker_nodes == 1
    assert node_type_configs["worker_nodes"].resources == {}
    assert node_type_configs["worker_nodes"].labels == {}

    assert node_type_configs["worker_nodes1"].max_worker_nodes == 2
    assert node_type_configs["worker_nodes1"].min_worker_nodes == 1
    assert node_type_configs["worker_nodes1"].resources == {"CPU": 2}
    assert node_type_configs["worker_nodes1"].labels == {"foo": "bar"}


@pytest.mark.parametrize(
    "env_value, expected",
    [(600, 600.0), (30, 30.0), (0, 0.0), (-1, None)],
)
def test_pending_idle_timeout_s(monkeypatch, env_value, expected):
    monkeypatch.setattr(
        config_mod, "RAY_AUTOSCALER_PENDING_NODE_IDLE_TIMEOUT_S", env_value
    )
    config = FileConfigReader(
        get_test_config_path("test_ray_complex.yaml")
    ).get_cached_autoscaling_config()
    assert config.get_pending_idle_timeout_s() == expected


def test_pending_idle_timeout_s_disabled_by_default(monkeypatch):
    monkeypatch.delenv("RAY_AUTOSCALER_PENDING_NODE_IDLE_TIMEOUT_S", raising=False)
    # Re-evaluate the constant without the env var set.
    constants = importlib.reload(
        importlib.import_module("ray.autoscaler._private.constants")
    )
    monkeypatch.setattr(
        config_mod,
        "RAY_AUTOSCALER_PENDING_NODE_IDLE_TIMEOUT_S",
        constants.RAY_AUTOSCALER_PENDING_NODE_IDLE_TIMEOUT_S,
    )
    config = FileConfigReader(
        get_test_config_path("test_ray_complex.yaml")
    ).get_cached_autoscaling_config()
    assert config.get_pending_idle_timeout_s() is None


@pytest.mark.parametrize("gated, expected", [(False, 600.0), (True, None)])
def test_pending_idle_timeout_s_gated(monkeypatch, gated, expected):
    monkeypatch.setattr(config_mod, "RAY_AUTOSCALER_PENDING_NODE_IDLE_TIMEOUT_S", 600)
    config = FileConfigReader(
        get_test_config_path("test_ray_complex.yaml")
    ).get_cached_autoscaling_config()
    if gated:
        provider_config = config.get_raw_config_mutable()["provider"]
        provider_config[PENDING_NODE_IDLE_TERMINATION_GATED_KEY] = True
    assert config.get_pending_idle_timeout_s() == expected


def test_read_config():
    # Make a temp config file from aws/defaults.yaml
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        # Write "aws/defaults.yaml" to the temp file
        with open(
            os.path.join(AUTOSCALER_DIR_PATH, "aws/defaults.yaml"), "r"
        ) as default_file:
            f.write(default_file.read())

    config_reader = FileConfigReader(f.name)

    # Check that the config is read correctly
    assert config_reader.get_cached_autoscaling_config().provider == Provider.AWS

    # Now override the file with a different provider
    with open(f.name, "w") as f:
        # Replace the file with "gcp/defaults.yaml"
        with open(
            os.path.join(AUTOSCALER_DIR_PATH, "gcp/defaults.yaml"), "r"
        ) as default_file:
            f.write(default_file.read())

    # Still the same.
    assert config_reader.get_cached_autoscaling_config().provider == Provider.AWS

    # Reload
    config_reader.refresh_cached_autoscaling_config()
    assert config_reader.get_cached_autoscaling_config().provider == Provider.GCP


def test_readonly_node_type_name_and_fallback(monkeypatch):
    class _DummyNodeState:
        def __init__(self, ray_node_type_name, node_id, total_resources):
            self.ray_node_type_name = ray_node_type_name
            self.node_id = node_id
            self.total_resources = total_resources

    class _DummyClusterState:
        def __init__(self, node_states):
            self.node_states = node_states

    # Avoid real GCS usage.
    monkeypatch.setattr(config_mod, "GcsClient", lambda address: object())
    # Build a cluster with:
    # - 1 named head type
    # - 2 named worker types of the same type (aggregation check)
    # - 1 worker type without name (fallback to node_id-based type)
    unnamed_worker_id = b"\xab"
    fallback_name = format_readonly_node_type(binary_to_hex(unnamed_worker_id))
    nodes = [
        _DummyNodeState(
            "ray.head.default", b"\x01", {"CPU": 1, "node:__internal_head__": 1}
        ),
        _DummyNodeState("worker.custom", b"\x02", {"CPU": 2}),
        _DummyNodeState("worker.custom", b"\x03", {"CPU": 2}),
        _DummyNodeState("worker.custom", b"\x04", {"CPU": 2}),
        _DummyNodeState("", unnamed_worker_id, {"CPU": 3}),
    ]
    monkeypatch.setattr(
        config_mod,
        "get_cluster_resource_state",
        lambda _gc: _DummyClusterState(nodes),
    )

    reader = ReadOnlyProviderConfigReader("dummy:0")
    reader.refresh_cached_autoscaling_config()
    cfg = reader.get_cached_autoscaling_config()

    node_types = cfg.get_config("available_node_types")
    # Head assertions
    assert "ray.head.default" in node_types
    assert node_types["ray.head.default"]["max_workers"] == 0
    assert cfg.get_head_node_type() == "ray.head.default"
    # Preferred name aggregation
    assert "worker.custom" in node_types
    assert node_types["worker.custom"]["max_workers"] == 3
    # Fallback for unnamed worker
    assert fallback_name in node_types
    assert node_types[fallback_name]["max_workers"] == 1

    # Global max_workers should be the sum of all worker-type max_workers,
    # NOT the count of node type names.
    # Here: 3 distinct types (head, worker.custom, fallback), but
    # 4 actual workers (3 x worker.custom + 1 x fallback).
    # The old buggy `len(available_node_types)` returned 3 instead of 4.
    assert cfg.get_config("max_workers") == 4


def _make_internal_replica_autoscaling_cfg(
    *,
    disable_launch_config_check: bool = True,
    node_type: str = "tpu",
    min_workers: int = 0,
    max_workers: int = 8,
    replica_config=None,
    head_replica_config=None,
) -> AutoscalingConfig:
    available = {
        "head": {"node_config": {}, "resources": {"CPU": 0}, "max_workers": 3},
    }
    if node_type != "head":
        available[node_type] = {
            "node_config": {},
            "resources": {"CPU": 4},
            "min_workers": min_workers,
            "max_workers": max_workers,
        }
    cfg = AutoscalingConfig(
        configs={
            "cluster_name": "test",
            "max_workers": max(max_workers, 8),
            "provider": {
                "type": "aws",
                "disable_launch_config_check": disable_launch_config_check,
            },
            "head_node_type": "head",
            "available_node_types": available,
        },
        skip_content_hash=True,
    )
    if replica_config is not None:
        cfg._configs["available_node_types"][node_type][
            "replica_config"
        ] = replica_config
    if head_replica_config is not None:
        cfg._configs["available_node_types"]["head"][
            "replica_config"
        ] = head_replica_config
    return cfg


def test_multihost_launch_config_check_rejected():
    autoscaling_cfg = _make_internal_replica_autoscaling_cfg(
        disable_launch_config_check=False,
        replica_config={"workers_per_replica": 4},
    )
    with pytest.raises(ValueError, match="workers_per_replica=4 > 1"):
        autoscaling_cfg.get_node_type_configs()

    # Single-host replica_config (workers_per_replica=1) is allowed even with
    # launch config checks enabled.
    single_host_cfg = _make_internal_replica_autoscaling_cfg(
        disable_launch_config_check=False,
        node_type="cpu",
        max_workers=3,
        replica_config={"workers_per_replica": 1},
    )
    cfgs = single_host_cfg.get_node_type_configs()
    assert cfgs["cpu"].workers_per_replica == 1
    assert cfgs["head"].workers_per_replica == 1

    # Head node type cannot set workers_per_replica > 1 even when max_workers + 1
    # would otherwise be divisible by workers_per_replica.
    head_multihost_cfg = _make_internal_replica_autoscaling_cfg(
        disable_launch_config_check=True,
        node_type="head",
        head_replica_config={"workers_per_replica": 4},
    )
    with pytest.raises(ValueError, match="Head node type head cannot set"):
        head_multihost_cfg.get_node_type_configs()


def test_multihost_replica_config_validation():
    import jsonschema

    from ray.autoscaler.v2.instance_manager.config import (
        NodeTypeConfig,
        WorkerReplicaConfig,
    )

    # Public ray-schema.json rejects `replica_config` until the replica-aware
    # scheduler lands (R5).
    with pytest.raises(jsonschema.exceptions.ValidationError):
        AutoscalingConfig(
            configs={
                "cluster_name": "test",
                "max_workers": 8,
                "provider": {"type": "aws", "disable_launch_config_check": True},
                "head_node_type": "head",
                "available_node_types": {
                    "head": {"node_config": {}, "resources": {"CPU": 0}},
                    "tpu": {
                        "node_config": {},
                        "resources": {"CPU": 4},
                        "min_workers": 4,
                        "max_workers": 8,
                        "replica_config": {"workers_per_replica": 4},
                    },
                },
            },
            skip_content_hash=True,
        )

    valid_cfg = _make_internal_replica_autoscaling_cfg(
        min_workers=4,
        max_workers=8,
        replica_config={"workers_per_replica": 4},
    )
    cfgs = valid_cfg.get_node_type_configs()
    assert cfgs["tpu"].replica_config == WorkerReplicaConfig(workers_per_replica=4)
    assert cfgs["tpu"].workers_per_replica == 4
    assert cfgs["head"].workers_per_replica == 1

    for bad_size in (0, -1, True, False, "4"):
        with pytest.raises(ValueError, match="workers_per_replica"):
            WorkerReplicaConfig(workers_per_replica=bad_size)

    with pytest.raises(ValueError, match=r"min_worker_nodes \(2\)"):
        NodeTypeConfig(
            name="tpu",
            resources={"CPU": 4},
            min_worker_nodes=2,
            max_worker_nodes=8,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )

    with pytest.raises(ValueError, match=r"max_worker_nodes \(6\)"):
        NodeTypeConfig(
            name="tpu",
            resources={"CPU": 4},
            min_worker_nodes=0,
            max_worker_nodes=6,
            replica_config=WorkerReplicaConfig(workers_per_replica=4),
        )

    # TG10: Size-multiple and field validation through AutoscalingConfig.get_node_type_configs.
    bad_min_cfg = _make_internal_replica_autoscaling_cfg(
        min_workers=2,
        max_workers=8,
        replica_config={"workers_per_replica": 4},
    )
    with pytest.raises(ValueError, match=r"min_worker_nodes \(2\)"):
        bad_min_cfg.get_node_type_configs()

    bad_max_cfg = _make_internal_replica_autoscaling_cfg(
        min_workers=0,
        max_workers=6,
        replica_config={"workers_per_replica": 4},
    )
    with pytest.raises(ValueError, match=r"max_worker_nodes \(6\)"):
        bad_max_cfg.get_node_type_configs()

    bad_type_cfg = _make_internal_replica_autoscaling_cfg(replica_config="not-a-dict")
    with pytest.raises(ValueError, match="must be a dict"):
        bad_type_cfg.get_node_type_configs()

    unknown_field_cfg = _make_internal_replica_autoscaling_cfg(
        replica_config={"workers_per_replica": 4, "unknown_field": True},
    )
    with pytest.raises(ValueError, match="Unknown replica_config fields"):
        unknown_field_cfg.get_node_type_configs()


def test_foreground_node_launch_uncaps_max_concurrent_launches_unless_overridden(
    monkeypatch,
):
    """Foreground node-launch providers (KubeRay) bypass the default 10-node per-tick
    cap unless AUTOSCALER_MAX_CONCURRENT_LAUNCHES is set, while Conservative
    upscaling_speed and non-foreground VM providers stay bounded."""
    from collections import Counter

    from ray.autoscaler._private.constants import (
        AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
        DISABLE_LAUNCH_CONFIG_CHECK_KEY,
        FOREGROUND_NODE_LAUNCH_KEY,
    )
    from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
    from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
    from ray.autoscaler.v2.instance_manager.node_provider import (
        CloudInstance,
        ICloudInstanceProvider,
    )
    from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
    from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
    from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
        CloudResourceMonitor,
    )
    from ray.autoscaler.v2.scheduler import ResourceDemandScheduler
    from ray.core.generated.autoscaler_pb2 import (
        ClusterResourceState,
        NodeState,
        NodeStatus,
        ResourceRequest,
        ResourceRequestByCount,
    )
    from ray.core.generated.instance_manager_pb2 import Instance, NodeKind

    monkeypatch.delenv("AUTOSCALER_MAX_CONCURRENT_LAUNCHES", raising=False)

    def _make_cfg(
        foreground: bool, upscaling_speed: float = 1000.0
    ) -> AutoscalingConfig:
        if foreground:
            from ray.autoscaler._private.kuberay.autoscaling_config import (
                _derive_autoscaling_config_from_ray_cr,
            )
            from ray.tests.kuberay.test_autoscaling_config import get_basic_ray_cr

            ray_cr = get_basic_ray_cr()
            ray_cr["metadata"]["namespace"] = "default"
            ray_cr["spec"]["headGroupSpec"]["rayStartParams"] = {"num-cpus": "0"}
            wg = ray_cr["spec"]["workerGroupSpecs"][0]
            wg.update(
                groupName="tpu-4h",
                replicas=0,
                minReplicas=0,
                maxReplicas=10,
                numOfHosts=4,
            )
            wg["template"]["spec"]["nodeSelector"] = {
                "cloud.google.com/gke-tpu-accelerator": "tpu-v6e-slice",
                "cloud.google.com/gke-tpu-topology": "4x4",
            }
            wg["template"]["spec"]["containers"][0]["resources"] = {
                "limits": {"cpu": "4", "google.com/tpu": "4"},
                "requests": {"cpu": "4", "google.com/tpu": "4"},
            }
            ray_cr["spec"]["workerGroupSpecs"] = [wg]
            raw = _derive_autoscaling_config_from_ray_cr(ray_cr)
            raw["upscaling_speed"] = upscaling_speed
            cfg = AutoscalingConfig(configs=raw, skip_content_hash=True)
            cfg._configs["available_node_types"]["tpu-4h"]["replica_config"] = {
                "workers_per_replica": 4
            }
            return cfg
        cfg = AutoscalingConfig(
            configs={
                "cluster_name": "test",
                "max_workers": 40,
                "upscaling_speed": upscaling_speed,
                "idle_timeout_minutes": 5,
                "provider": {
                    "type": "aws",
                    DISABLE_LAUNCH_CONFIG_CHECK_KEY: True,
                    FOREGROUND_NODE_LAUNCH_KEY: False,
                },
                "head_node_type": "headgroup",
                "available_node_types": {
                    "headgroup": {"node_config": {}, "resources": {"CPU": 0}},
                    "tpu-4h": {
                        "node_config": {},
                        "resources": {"CPU": 4, "TPU": 4},
                        "min_workers": 0,
                        "max_workers": 40,
                    },
                },
            },
            skip_content_hash=True,
        )
        cfg._configs["available_node_types"]["tpu-4h"]["replica_config"] = {
            "workers_per_replica": 4
        }
        return cfg

    fg_cfg = _make_cfg(foreground=True, upscaling_speed=1000.0)
    vm_cfg = _make_cfg(foreground=False, upscaling_speed=1000.0)
    assert fg_cfg.get_max_concurrent_launches() == sys.maxsize
    assert vm_cfg.get_max_concurrent_launches() == AUTOSCALER_MAX_CONCURRENT_LAUNCHES

    monkeypatch.setenv("AUTOSCALER_MAX_CONCURRENT_LAUNCHES", "24")
    assert fg_cfg.get_max_concurrent_launches() == 24
    assert vm_cfg.get_max_concurrent_launches() == 24
    monkeypatch.delenv("AUTOSCALER_MAX_CONCURRENT_LAUNCHES")

    class _NoopProvider(ICloudInstanceProvider):
        def get_non_terminated(self):
            return {"hpod": CloudInstance("hpod", "headgroup", NodeKind.HEAD, True)}

        def terminate(self, ids, request_id):
            return None

        def launch(self, shape, request_id):
            return None

        def poll_errors(self):
            return []

    def _run_cold_start_tick(acfg: AutoscalingConfig) -> Counter:
        storage = InstanceStorage(cluster_id="fg", storage=InMemoryStorage())
        im = InstanceManager(
            instance_storage=storage, instance_status_update_subscribers=[]
        )
        hnid = b"h".ljust(28, b"\0")
        head_im = Instance(
            instance_id="h0",
            instance_type="headgroup",
            status=Instance.RAY_RUNNING,
            node_kind=NodeKind.HEAD,
            node_id=hnid.hex(),
            cloud_instance_id="hpod",
            version=0,
        )
        assert storage.batch_upsert_instances([head_im], expected_storage_version=0)[0]
        crs = ClusterResourceState(
            node_states=[
                NodeState(
                    node_id=hnid,
                    instance_id="hpod",
                    ray_node_type_name="headgroup",
                    status=NodeStatus.RUNNING,
                )
            ],
            pending_resource_requests=[
                ResourceRequestByCount(
                    request=ResourceRequest(resources_bundle={"CPU": 1, "TPU": 4}),
                    count=20,
                )
            ],
        )
        Reconciler.reconcile(
            instance_manager=im,
            scheduler=ResourceDemandScheduler(),
            cloud_provider=_NoopProvider(),
            cloud_resource_monitor=CloudResourceMonitor(),
            ray_cluster_resource_state=crs,
            non_terminated_cloud_instances={
                "hpod": CloudInstance("hpod", "headgroup", NodeKind.HEAD, True)
            },
            cloud_provider_errors=[],
            ray_install_errors=[],
            autoscaling_config=acfg,
        )
        insts, _ = storage.get_instances()
        return Counter(i.status for i in insts.values() if i.node_kind != NodeKind.HEAD)

    # Aggressive KubeRay cold start launches all 20 workers (5 x 4-host slices) in 1 tick.
    assert _run_cold_start_tick(fg_cfg) == {Instance.REQUESTED: 20}
    # Conservative KubeRay cold start (upscaling_speed=1.0) still rounds 1 worker up to 1 slice (4 REQUESTED, 16 QUEUED).
    cons_cfg = _make_cfg(foreground=True, upscaling_speed=1.0)
    assert _run_cold_start_tick(cons_cfg) == {
        Instance.REQUESTED: 4,
        Instance.QUEUED: 16,
    }


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))

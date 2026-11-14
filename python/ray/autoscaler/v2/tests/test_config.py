# coding: utf-8
import importlib
import os
import sys
import tempfile

import pytest  # noqa

from ray._common.utils import binary_to_hex
from ray._private.test_utils import get_test_config_path
from ray.autoscaler import AUTOSCALER_DIR_PATH
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


def test_multihost_launch_config_check_rejected():
    autoscaling_cfg = AutoscalingConfig(
        configs={
            "provider": {"type": "aws", "disable_launch_config_check": False},
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {}, "resources": {"CPU": 0}},
                "tpu": {
                    "node_config": {},
                    "resources": {"CPU": 4, "TPU": 4},
                    "min_workers": 0,
                    "max_workers": 8,
                    "replica_config": {"workers_per_replica": 4},
                },
            },
        }
    )
    with pytest.raises(ValueError, match="workers_per_replica=4 > 1"):
        autoscaling_cfg.get_node_type_configs()

    # Single-host replica_config (workers_per_replica=1) is allowed even with
    # launch config checks enabled.
    single_host_cfg = AutoscalingConfig(
        configs={
            "provider": {"type": "aws", "disable_launch_config_check": False},
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {}, "resources": {"CPU": 0}},
                "cpu": {
                    "node_config": {},
                    "resources": {"CPU": 4},
                    "min_workers": 0,
                    "max_workers": 3,
                    "replica_config": {"workers_per_replica": 1},
                },
            },
        }
    )
    cfgs = single_host_cfg.get_node_type_configs()
    assert cfgs["cpu"].replica_config.workers_per_replica == 1

    # Head node type cannot set workers_per_replica > 1 even when max_workers + 1
    # would otherwise be divisible by workers_per_replica.
    head_multihost_cfg = AutoscalingConfig(
        configs={
            "provider": {"type": "aws", "disable_launch_config_check": True},
            "head_node_type": "head",
            "available_node_types": {
                "head": {
                    "node_config": {},
                    "resources": {"CPU": 0},
                    "min_workers": 0,
                    "max_workers": 3,
                    "replica_config": {"workers_per_replica": 4},
                },
            },
        }
    )
    with pytest.raises(ValueError, match="Head node type head cannot set"):
        head_multihost_cfg.get_node_type_configs()


def test_multihost_replica_config_validation():
    import jsonschema

    from ray.autoscaler.v2.instance_manager.config import (
        NodeTypeConfig,
        WorkerReplicaConfig,
    )

    valid_cfg = AutoscalingConfig(
        configs={
            "cluster_name": "test",
            "max_workers": 8,
            "provider": {"type": "aws", "disable_launch_config_check": True},
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {}, "resources": {"CPU": 0}},
                "tpu": {
                    "node_config": {},
                    "resources": {"CPU": 4, "TPU": 4},
                    "min_workers": 4,
                    "max_workers": 8,
                    "replica_config": {"workers_per_replica": 4},
                },
            },
        }
    )
    cfgs = valid_cfg.get_node_type_configs()
    assert cfgs["tpu"].replica_config == WorkerReplicaConfig(workers_per_replica=4)

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

    with pytest.raises(jsonschema.exceptions.ValidationError):
        AutoscalingConfig(
            configs={
                "cluster_name": "test",
                "provider": {"type": "aws", "disable_launch_config_check": True},
                "head_node_type": "head",
                "available_node_types": {
                    "head": {"node_config": {}, "resources": {"CPU": 0}},
                    "tpu": {
                        "node_config": {},
                        "resources": {"CPU": 4},
                        "replica_config": {
                            "workers_per_replica": 4,
                            "unknown_field": True,
                        },
                    },
                },
            }
        )


def test_replica_config_locality_fields_and_runtime_label_keys():
    import jsonschema

    from ray.autoscaler.v2.instance_manager.config import (
        NodeTypeConfig,
        WorkerReplicaConfig,
    )

    # Positive round-trip: multi-host TPU with deduplication, single-host TPU,
    # explicit JSON nulls, and omitted defaults.
    valid_cfg = AutoscalingConfig(
        configs={
            "cluster_name": "test",
            "max_workers": 16,
            "provider": {"type": "aws", "disable_launch_config_check": True},
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {}, "resources": {"CPU": 0}},
                "tpu-mh": {
                    "node_config": {},
                    "resources": {"CPU": 4, "TPU": 4, "TPU-v6e-16-head": 1},
                    "min_workers": 4,
                    "max_workers": 8,
                    "runtime_label_keys": [
                        "ray.io/tpu-slice-name",
                        "ray.io/tpu-worker-id",
                        "ray.io/tpu-slice-name",
                    ],
                    "replica_config": {
                        "workers_per_replica": 4,
                        "domain_label_key": "ray.io/tpu-slice-name",
                        "domain_exclusive": True,
                        "worker_index_label_key": "ray.io/tpu-worker-id",
                        "head_resources": ["TPU-v6e-16-head", "TPU-v6e-16-head"],
                    },
                },
                "tpu-sh": {
                    "node_config": {},
                    "resources": {"CPU": 4, "TPU": 4},
                    "min_workers": 0,
                    "max_workers": 4,
                    "replica_config": {
                        "workers_per_replica": 1,
                        "domain_label_key": "ray.io/tpu-slice-name",
                        "worker_index_label_key": "ray.io/tpu-worker-id",
                        "head_resources": ["TPU-v6e-4-head"],
                    },
                },
                "explicit-nulls": {
                    "node_config": {},
                    "resources": {"CPU": 4},
                    "min_workers": 0,
                    "max_workers": 2,
                    "replica_config": {
                        "workers_per_replica": 1,
                        "domain_label_key": None,
                        "domain_exclusive": False,
                        "worker_index_label_key": None,
                    },
                },
                "defaults-only": {
                    "node_config": {},
                    "resources": {"CPU": 2},
                    "min_workers": 0,
                    "max_workers": 2,
                },
            },
        }
    )
    cfgs = valid_cfg.get_node_type_configs()
    assert cfgs["tpu-mh"].replica_config == WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key="ray.io/tpu-slice-name",
        domain_exclusive=True,
        worker_index_label_key="ray.io/tpu-worker-id",
        head_resources=("TPU-v6e-16-head",),
    )
    assert cfgs["tpu-mh"].runtime_label_keys == (
        "ray.io/tpu-slice-name",
        "ray.io/tpu-worker-id",
    )
    assert cfgs["tpu-sh"].replica_config == WorkerReplicaConfig(
        workers_per_replica=1,
        domain_label_key="ray.io/tpu-slice-name",
        domain_exclusive=True,
        worker_index_label_key="ray.io/tpu-worker-id",
        head_resources=("TPU-v6e-4-head",),
    )
    assert cfgs["tpu-sh"].runtime_label_keys == ()
    assert cfgs["explicit-nulls"].replica_config == WorkerReplicaConfig(
        workers_per_replica=1,
        domain_label_key=None,
        domain_exclusive=False,
        worker_index_label_key=None,
        head_resources=(),
    )
    assert cfgs["defaults-only"].replica_config is None
    assert cfgs["defaults-only"].runtime_label_keys == ()

    # workers_per_replica == 1 with locality metadata works without
    # disable_launch_config_check.
    single_host_clique_cfg = AutoscalingConfig(
        configs={
            "cluster_name": "test",
            "max_workers": 4,
            "provider": {"type": "aws", "disable_launch_config_check": False},
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {}, "resources": {"CPU": 0}},
                "gpu-clique": {
                    "node_config": {},
                    "resources": {"CPU": 8, "GPU": 8},
                    "min_workers": 0,
                    "max_workers": 4,
                    "runtime_label_keys": ["ray.io/gpu-domain"],
                    "replica_config": {
                        "workers_per_replica": 1,
                        "domain_label_key": "ray.io/gpu-domain",
                        "domain_exclusive": False,
                    },
                },
            },
        }
    )
    sh_cfgs = single_host_clique_cfg.get_node_type_configs()
    assert sh_cfgs["gpu-clique"].replica_config == WorkerReplicaConfig(
        workers_per_replica=1,
        domain_label_key="ray.io/gpu-domain",
        domain_exclusive=False,
    )
    assert sh_cfgs["gpu-clique"].runtime_label_keys == ("ray.io/gpu-domain",)

    # Negative Python dataclass validation.
    for bad_excl in (1, 0, "true", "false", None):
        with pytest.raises(ValueError, match="domain_exclusive"):
            WorkerReplicaConfig(domain_exclusive=bad_excl)

    for bad_key in ("", "   ", 123, True, []):
        with pytest.raises(ValueError, match="domain_label_key"):
            WorkerReplicaConfig(domain_label_key=bad_key)
        with pytest.raises(ValueError, match="worker_index_label_key"):
            WorkerReplicaConfig(worker_index_label_key=bad_key)

    with pytest.raises(ValueError, match="must be distinct"):
        WorkerReplicaConfig(
            domain_label_key="ray.io/same",
            worker_index_label_key="ray.io/same",
        )

    for bad_heads in (
        "TPU-head",
        None,
        {"TPU": 1},
        {"TPU"},
        123,
        [""],
        ["  "],
        [1],
        [None],
    ):
        with pytest.raises(ValueError, match="head_resources"):
            WorkerReplicaConfig(head_resources=bad_heads)

    for bad_rt in (
        "ray.io/x",
        None,
        {"a": 1},
        {"a"},
        123,
        [""],
        ["  "],
        [123],
        [None],
    ):
        with pytest.raises(ValueError, match="runtime_label_keys"):
            NodeTypeConfig(
                name="worker",
                min_worker_nodes=0,
                max_worker_nodes=2,
                resources={"CPU": 4},
                runtime_label_keys=bad_rt,
            )

    # Raw dict checks when schema validation is bypassed (mutated _configs).
    for mutated_fields, match in (
        ({"replica_config": "invalid"}, "replica_config must be a dict"),
        ({"replica_config": {"head_resources": None}}, "head_resources"),
        ({"replica_config": {"domain_exclusive": None}}, "domain_exclusive"),
        ({"runtime_label_keys": None}, "runtime_label_keys"),
    ):
        cfg = AutoscalingConfig(
            configs={
                "cluster_name": "test",
                "provider": {"type": "aws", "disable_launch_config_check": True},
                "head_node_type": "head",
                "available_node_types": {
                    "head": {"node_config": {}, "resources": {"CPU": 0}},
                    "worker": {"node_config": {}, "resources": {"CPU": 4}},
                },
            }
        )
        cfg._configs["available_node_types"]["worker"].update(mutated_fields)
        with pytest.raises(ValueError, match=match):
            cfg.get_node_type_configs()

    # Negative JSON Schema validation.
    def _make_schema_cfg(worker_overrides):
        worker = {"node_config": {}, "resources": {"CPU": 4}}
        worker.update(worker_overrides)
        return {
            "cluster_name": "test",
            "provider": {"type": "aws", "disable_launch_config_check": True},
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {}, "resources": {"CPU": 0}},
                "worker": worker,
            },
        }

    invalid_schema_cases = [
        {"replica_config": {"domain_exclusive": 1}},
        {"replica_config": {"domain_exclusive": "true"}},
        {"replica_config": {"domain_exclusive": None}},
        {"replica_config": {"domain_label_key": ""}},
        {"replica_config": {"domain_label_key": 123}},
        {"replica_config": {"worker_index_label_key": ""}},
        {"replica_config": {"worker_index_label_key": 123}},
        {"replica_config": {"head_resources": "TPU-head"}},
        {"replica_config": {"head_resources": [""]}},
        {"replica_config": {"head_resources": [1]}},
        {"replica_config": {"head_resources": None}},
        {"runtime_label_keys": "ray.io/x"},
        {"runtime_label_keys": [""]},
        {"runtime_label_keys": [1]},
        {"runtime_label_keys": None},
    ]
    for case in invalid_schema_cases:
        with pytest.raises(jsonschema.exceptions.ValidationError):
            AutoscalingConfig(configs=_make_schema_cfg(case))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))

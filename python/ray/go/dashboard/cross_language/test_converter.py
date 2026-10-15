import threading
from unittest import mock

import msgpack
import pytest

from ray.go.dashboard.cross_language.converter import (
    parse_deployment_id,
    parse_serve_deploy_schema,
    to_msgpack_native,
)
from ray.serve._private.common import DeploymentID
from ray.serve.schema import (
    APIType,
    ApplicationDetails,
    ApplicationStatus,
    ServeInstanceDetails,
)
from ray.train._internal.state.schema import (
    ActorStatusEnum,
    RunStatusEnum,
    TrainRunInfo,
    TrainWorkerInfo,
)
from ray.train.v2._internal.state.schema import (
    BackendConfig as V2BackendConfigSchema,
    CheckpointConfig as V2CheckpointConfigSchema,
    DataConfig as V2DataConfigSchema,
    DataExecutionOptions as V2DataExecutionOptions,
    ExecutionOptions as V2ExecutionOptions,
    FailureConfig as V2FailureConfigSchema,
    RunConfig as V2RunConfigSchema,
    RunSettings as V2RunSettings,
    RunStatus,
    ScalingConfig as V2ScalingConfigSchema,
    TrainResources,
    TrainRun,
    TrainWorker,
)


def _minimal_train_run_settings() -> V2RunSettings:
    """Build a minimal valid RunSettings for TrainRun constructions.

    TrainRun requires framework_versions and run_settings with no defaults, so
    the tests must supply them. This mirrors the construction used by the
    ray.train.v2 tests (train/v2/tests/util.py, test_state.py).
    """
    return V2RunSettings(
        train_loop_config=None,
        backend_config=V2BackendConfigSchema(framework=None, config={}),
        scaling_config=V2ScalingConfigSchema(
            num_workers=1,
            use_gpu=False,
            placement_strategy="PACK",
            use_tpu=False,
        ),
        datasets=[],
        data_config=V2DataConfigSchema(
            datasets_to_split="all",
            data_execution_options=V2DataExecutionOptions(
                default=V2ExecutionOptions(
                    resource_limits={},
                    exclude_resources={},
                    preserve_order=False,
                    actor_locality_enabled=False,
                    verbose_progress=False,
                ),
            ),
            enable_shard_locality=True,
        ),
        run_config=V2RunConfigSchema(
            name="test",
            failure_config=V2FailureConfigSchema(
                max_failures=0, controller_failure_limit=-1
            ),
            worker_runtime_env={},
            checkpoint_config=V2CheckpointConfigSchema(checkpoint_score_order="max"),
            storage_path="/tmp/ray",
        ),
    )


def _strict_dumps(obj):
    """msgpack.dumps with strict_types=True, exactly what the Go cross-language
    serializer requires (any residual Enum/BaseModel raises TypeError)."""
    return msgpack.dumps(obj, strict_types=True)


def test_train_v2_run_enums_converted():
    run = TrainRun(
        id="r1",
        name="run",
        status=RunStatus.RUNNING,
        job_id="j1",
        controller_actor_id="c1",
        start_time_ns=0,
        end_time_ns=None,
        framework_versions={"ray": "3.0.0.dev0"},
        run_settings=_minimal_train_run_settings(),
    )
    out = to_msgpack_native(run.dict())
    _strict_dumps(out)
    assert out["status"] == "RUNNING"


def test_train_v2_worker_nested_resources_converted():
    worker = TrainWorker(
        world_rank=0,
        local_rank=0,
        node_rank=0,
        actor_id="a1",
        node_id="n1",
        node_ip="1.1.1.1",
        pid=1,
        gpu_ids=[],
        status=None,
        resources=TrainResources(resources={"CPU": 1.0}),
        log_file_path=None,
    )
    out = to_msgpack_native(worker.dict())
    _strict_dumps(out)
    assert out["resources"]["resources"] == {"CPU": 1.0}


def test_train_v1_run_info_enums_converted():
    worker = TrainWorkerInfo(
        actor_id="a1",
        world_rank=0,
        local_rank=0,
        node_rank=0,
        node_id="n1",
        node_ip="1.1.1.1",
        pid=1,
        gpu_ids=[],
        status=ActorStatusEnum.ALIVE,
        resources={"CPU": 1.0},
    )
    info = TrainRunInfo(
        name="n1",
        id="r1",
        job_id="j1",
        controller_actor_id="c1",
        workers=[worker],
        datasets=[],
        run_status=RunStatusEnum.RUNNING,
        status_detail="",
        start_time_ms=0,
        end_time_ms=None,
        resources=[{"CPU": 1.0}],
    )
    out = to_msgpack_native(info.dict())
    _strict_dumps(out)
    assert out["run_status"] == "RUNNING"
    assert out["workers"][0]["status"] == "ALIVE"


def test_serve_instance_details_enums_converted():
    instance = ServeInstanceDetails(
        controller_info={},
        proxies={},
        applications={
            "app1": ApplicationDetails(
                name="app1",
                route_prefix="/",
                docs_path=None,
                status=ApplicationStatus.RUNNING,
                message="",
                last_deployed_time_s=0.0,
                deployments={},
                source=APIType.IMPERATIVE,
                external_scaler_enabled=False,
            )
        },
    )
    out = to_msgpack_native(
        instance._get_user_facing_json_serializable_dict(exclude_unset=True)
    )
    _strict_dumps(out)
    assert out["applications"]["app1"]["status"] == "RUNNING"
    assert out["applications"]["app1"]["source"] == "imperative"


def test_plain_native_passthrough():
    native = {"a": 1, "b": [1.5, True, None, "x", b"\x00"], "c": {"d": 2}}
    out = to_msgpack_native(native)
    _strict_dumps(out)
    assert out == native


def test_parse_serve_deploy_schema_valid():
    config = {
        "applications": [
            {"name": "app1", "import_path": "pkg.dag"},
        ]
    }
    schema = parse_serve_deploy_schema(config)
    assert len(schema.applications) == 1
    assert schema.applications[0].name == "app1"
    assert schema.applications[0].import_path == "pkg.dag"


def test_parse_serve_deploy_schema_missing_applications():
    with pytest.raises(ValueError):
        parse_serve_deploy_schema({})


def test_parse_serve_deploy_schema_duplicate_name():
    with pytest.raises(ValueError):
        parse_serve_deploy_schema(
            {
                "applications": [
                    {"name": "a", "import_path": "pkg1.dag"},
                    {"name": "a", "import_path": "pkg2.dag"},
                ]
            }
        )


def test_parse_serve_deploy_schema_duplicate_route():
    with pytest.raises(ValueError):
        parse_serve_deploy_schema(
            {
                "applications": [
                    {"name": "a", "import_path": "pkg1.dag", "route_prefix": "/r"},
                    {"name": "b", "import_path": "pkg2.dag", "route_prefix": "/r"},
                ]
            }
        )


def test_parse_serve_deploy_schema_empty_name():
    with pytest.raises(ValueError):
        parse_serve_deploy_schema(
            {"applications": [{"name": "", "import_path": "pkg.dag"}]}
        )


def test_parse_deployment_id_full():
    did = parse_deployment_id({"name": "dep1", "app_name": "app1"})
    assert did == DeploymentID(name="dep1", app_name="app1")


def test_parse_deployment_id_default_app_name():
    did = parse_deployment_id({"name": "dep1"})
    assert did == DeploymentID(name="dep1", app_name="default")


def test_parse_deployment_id_missing_name():
    with pytest.raises(KeyError):
        parse_deployment_id({"app_name": "app1"})


def test_to_msgpack_native_parse_deploy_schema_roundtrip():
    # The schema constructed from a Go dict must serialize as msgpack-native
    # (i.e. no residual BaseModel/Enum), exactly what apply_config_native hands
    # back to Go.
    config = {"applications": [{"name": "app1", "import_path": "pkg.dag"}]}
    schema = parse_serve_deploy_schema(config)
    out = to_msgpack_native(schema.dict())
    _strict_dumps(out)
    assert out["applications"][0]["name"] == "app1"


def _mock_controller_with_method(method_name, result, **attrs):
    """Builds a minimal mock object exposing the original controller method."""
    controller = mock.Mock()
    getattr(controller, method_name).return_value = result
    for name, value in attrs.items():
        setattr(controller, name, value)
    return controller


def test_serve_get_serve_instance_details_native():
    from ray.go.dashboard.cross_language import converter
    from ray.serve._private.controller import ServeController

    instance = ServeInstanceDetails(
        controller_info={},
        proxies={},
        applications={
            "app1": ApplicationDetails(
                name="app1",
                route_prefix="/",
                docs_path=None,
                status=ApplicationStatus.RUNNING,
                message="",
                last_deployed_time_s=0.0,
                deployments={},
                source=APIType.IMPERATIVE,
                external_scaler_enabled=False,
            )
        },
    )
    controller = _mock_controller_with_method("get_serve_instance_details", instance)
    with mock.patch.object(
        converter,
        "to_msgpack_native",
        wraps=converter.to_msgpack_native,
    ):
        out = ServeController.get_serve_instance_details_native(controller)
    _strict_dumps(out)
    assert out["applications"]["app1"]["status"] == "RUNNING"


def test_serve_apply_config_native():
    from ray.go.dashboard.cross_language import converter
    from ray.serve._private.controller import ServeController
    from ray.serve.schema import ServeDeploySchema

    config = {"applications": [{"name": "app1", "import_path": "pkg.dag"}]}
    controller = _mock_controller_with_method("apply_config", None)
    with mock.patch.object(
        converter,
        "parse_serve_deploy_schema",
        wraps=converter.parse_serve_deploy_schema,
    ):
        ServeController.apply_config_native(controller, config, 1.5)
    controller.apply_config.assert_called_once()
    parsed = controller.apply_config.call_args.args[0]
    assert isinstance(parsed, ServeDeploySchema)
    assert controller.apply_config.call_args.args[1] == 1.5


def test_serve_update_deployment_replicas_native():
    from ray.go.dashboard.cross_language import converter
    from ray.serve._private.common import DeploymentID
    from ray.serve._private.controller import ServeController

    controller = _mock_controller_with_method("update_deployment_replicas", None)
    with mock.patch.object(
        converter,
        "parse_deployment_id",
        wraps=converter.parse_deployment_id,
    ):
        ServeController.update_deployment_replicas_native(
            controller, {"name": "dep1", "app_name": "app1"}, 3
        )
    controller.update_deployment_replicas.assert_called_once()
    parsed = controller.update_deployment_replicas.call_args.args[0]
    assert parsed == DeploymentID(name="dep1", app_name="app1")
    assert controller.update_deployment_replicas.call_args.args[1] == 3


def test_train_v2_get_train_runs_native():
    from ray.go.dashboard.cross_language import converter
    from ray.train.v2._internal.state.schema import RunStatus, TrainRun
    from ray.train.v2._internal.state.state_actor import TrainStateActor

    run = TrainRun(
        id="r1",
        name="run",
        status=RunStatus.RUNNING,
        job_id="j1",
        controller_actor_id="c1",
        start_time_ns=0,
        end_time_ns=None,
        framework_versions={"ray": "3.0.0.dev0"},
        run_settings=_minimal_train_run_settings(),
    )
    actor = _mock_controller_with_method(
        "get_train_runs",
        {"r1": run},
        _runs_lock=threading.RLock(),
        _runs={"r1": run},
    )
    with mock.patch.object(
        converter,
        "to_msgpack_native",
        wraps=converter.to_msgpack_native,
    ):
        out = TrainStateActor.get_train_runs_native(actor)
    _strict_dumps(out)
    assert out["r1"]["status"] == "RUNNING"


def test_train_v2_get_train_run_attempts_native():
    from ray.go.dashboard.cross_language import converter
    from ray.train.v2._internal.state.schema import RunStatus, TrainRunAttempt
    from ray.train.v2._internal.state.state_actor import TrainStateActor

    attempt = TrainRunAttempt(
        run_id="r1",
        attempt_id="a1",
        status=RunStatus.RUNNING,
        status_detail="",
        start_time_ns=0,
        end_time_ns=None,
        resources=[],
        workers=[],
    )
    actor = _mock_controller_with_method(
        "get_train_run_attempts",
        {"r1": {"a1": attempt}},
        _run_attempts_lock=threading.RLock(),
        _run_attempts={"r1": {"a1": attempt}},
    )
    with mock.patch.object(
        converter,
        "to_msgpack_native",
        wraps=converter.to_msgpack_native,
    ):
        out = TrainStateActor.get_train_run_attempts_native(actor)
    _strict_dumps(out)
    assert out["r1"]["a1"]["status"] == "RUNNING"


def test_train_v1_get_all_train_runs_native():
    from ray.go.dashboard.cross_language import converter
    from ray.train._internal.state.schema import (
        ActorStatusEnum,
        RunStatusEnum,
        TrainRunInfo,
        TrainWorkerInfo,
    )
    from ray.train._internal.state.state_actor import TrainStateActor as V1ActorClass

    # V1 TrainStateActor is @ray.remote-decorated (an ActorClass); unwrap the
    # original class to reach the plain method for the thin-method test.
    TrainStateActor = V1ActorClass.__ray_metadata__.modified_class

    worker = TrainWorkerInfo(
        actor_id="a1",
        world_rank=0,
        local_rank=0,
        node_rank=0,
        node_id="n1",
        node_ip="1.1.1.1",
        pid=1,
        gpu_ids=[],
        status=ActorStatusEnum.ALIVE,
        resources={"CPU": 1.0},
    )
    info = TrainRunInfo(
        name="n1",
        id="r1",
        job_id="j1",
        controller_actor_id="c1",
        workers=[worker],
        datasets=[],
        run_status=RunStatusEnum.RUNNING,
        status_detail="",
        start_time_ms=0,
        end_time_ms=None,
        resources=[{"CPU": 1.0}],
    )
    actor = _mock_controller_with_method(
        "get_all_train_runs",
        {"r1": info},
        _run_infos={"r1": info},
    )
    with mock.patch.object(
        converter,
        "to_msgpack_native",
        wraps=converter.to_msgpack_native,
    ):
        out = TrainStateActor.get_all_train_runs_native(actor)
    _strict_dumps(out)
    assert out["r1"]["run_status"] == "RUNNING"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

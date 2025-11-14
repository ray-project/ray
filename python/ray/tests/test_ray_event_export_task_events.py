import base64
import json
import logging
from typing import Optional

import grpc
import pytest

import ray
import ray.dashboard.consts as dashboard_consts
from ray._common.network_utils import find_free_port
from ray._common.test_utils import wait_for_condition
from ray._private import ray_constants
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray._raylet import GcsClient

logger = logging.getLogger(__name__)


_EVENT_AGGREGATOR_AGENT_TARGET_PORT = find_free_port()
_EVENT_AGGREGATOR_AGENT_TARGET_IP = "127.0.0.1"
_EVENT_AGGREGATOR_AGENT_TARGET_ADDR = (
    f"http://{_EVENT_AGGREGATOR_AGENT_TARGET_IP}:{_EVENT_AGGREGATOR_AGENT_TARGET_PORT}"
)


@pytest.fixture(scope="module")
def httpserver_listen_address():
    return (_EVENT_AGGREGATOR_AGENT_TARGET_IP, _EVENT_AGGREGATOR_AGENT_TARGET_PORT)


_cluster_with_aggregator_target = pytest.mark.parametrize(
    ("preserve_proto_field_name", "ray_start_cluster_head_with_env_vars"),
    [
        pytest.param(
            preserve_proto_field_name,
            {
                "env_vars": {
                    "RAY_task_events_report_interval_ms": 100,
                    "RAY_enable_core_worker_ray_event_to_aggregator": "1",
                    "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
                    "RAY_DASHBOARD_AGGREGATOR_AGENT_PRESERVE_PROTO_FIELD_NAME": (
                        "1" if preserve_proto_field_name is True else "0"
                    ),
                },
            },
        )
        for preserve_proto_field_name in [True, False]
    ],
    indirect=["ray_start_cluster_head_with_env_vars"],
)


def wait_until_grpc_channel_ready(
    gcs_address: str, node_ids: list[str], timeout: int = 5
):
    # get the grpc port
    gcs_client = GcsClient(address=gcs_address)

    def get_dashboard_agent_address(node_id: str):
        return gcs_client.internal_kv_get(
            f"{ray.dashboard.consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id}".encode(),
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
        )

    wait_for_condition(
        lambda: all(
            get_dashboard_agent_address(node_id) is not None for node_id in node_ids
        )
    )
    grpc_ports = [
        json.loads(get_dashboard_agent_address(node_id))[2] for node_id in node_ids
    ]
    targets = [f"127.0.0.1:{grpc_port}" for grpc_port in grpc_ports]

    # wait for the dashboard agent grpc port to be ready
    for target in targets:
        channel = grpc.insecure_channel(target)
        try:
            grpc.channel_ready_future(channel).result(timeout=timeout)
        except grpc.FutureTimeoutError:
            return False
    return True


def get_job_id_and_driver_script_task_id_from_events(
    events: json, preserve_proto_field_name: bool
) -> tuple[Optional[str], Optional[str]]:
    test_job_id = base64.b64encode(
        ray.JobID.from_hex(ray.get_runtime_context().get_job_id()).binary()
    ).decode()
    driver_script_job_id = None
    driver_task_id = None
    for event in events:
        if preserve_proto_field_name:
            if event["event_type"] == "TASK_DEFINITION_EVENT":
                if (
                    event["task_definition_event"]["task_type"] == "DRIVER_TASK"
                    and event["task_definition_event"]["job_id"] != test_job_id
                ):
                    driver_task_id = event["task_definition_event"]["task_id"]
                    driver_script_job_id = event["task_definition_event"]["job_id"]
                    assert driver_task_id is not None
                    assert driver_script_job_id is not None
        else:
            if event["eventType"] == "TASK_DEFINITION_EVENT":
                if (
                    event["taskDefinitionEvent"]["taskType"] == "DRIVER_TASK"
                    and event["taskDefinitionEvent"]["jobId"] != test_job_id
                ):
                    driver_task_id = event["taskDefinitionEvent"]["taskId"]
                    driver_script_job_id = event["taskDefinitionEvent"]["jobId"]
                    assert driver_task_id is not None
                    assert driver_script_job_id is not None

    return driver_script_job_id, driver_task_id


def check_task_event_base_fields(event: json, preserve_proto_field_name: bool):
    assert event["timestamp"] is not None
    assert event["severity"] == "INFO"
    if preserve_proto_field_name:
        assert event["event_id"] is not None
        assert event["source_type"] == "CORE_WORKER"
        assert event["session_name"] is not None
    else:
        assert event["eventId"] is not None
        assert event["sourceType"] == "CORE_WORKER"
        assert event["sessionName"] is not None


def check_task_lifecycle_event_states_and_error_info(
    events: json,
    expected_task_id_states_dict: dict,
    expected_task_id_error_info_dict: dict,
    preserve_proto_field_name: bool,
):

    task_id_states_dict = {}
    task_id_error_info_dict = {}
    for event in events:
        if preserve_proto_field_name:
            if event["event_type"] == "TASK_LIFECYCLE_EVENT":
                task_id = event["task_lifecycle_event"]["task_id"]
                task_attempt = event["task_lifecycle_event"]["task_attempt"]
                if (task_id, task_attempt) not in task_id_states_dict:
                    task_id_states_dict[(task_id, task_attempt)] = set()

                for state in event["task_lifecycle_event"]["state_transitions"]:
                    task_id_states_dict[(task_id, task_attempt)].add(state["state"])

                if "ray_error_info" in event["task_lifecycle_event"]:
                    task_id_error_info_dict[(task_id, task_attempt)] = event[
                        "task_lifecycle_event"
                    ]["ray_error_info"]
        else:
            if event["eventType"] == "TASK_LIFECYCLE_EVENT":
                task_id = event["taskLifecycleEvent"]["taskId"]
                task_attempt = event["taskLifecycleEvent"]["taskAttempt"]
                if (task_id, task_attempt) not in task_id_states_dict:
                    task_id_states_dict[(task_id, task_attempt)] = set()

                for state in event["taskLifecycleEvent"]["stateTransitions"]:
                    task_id_states_dict[(task_id, task_attempt)].add(state["state"])

                if "rayErrorInfo" in event["taskLifecycleEvent"]:
                    task_id_error_info_dict[(task_id, task_attempt)] = event[
                        "taskLifecycleEvent"
                    ]["rayErrorInfo"]

    for (
        expected_task_id_attempt,
        expected_states,
    ) in expected_task_id_states_dict.items():
        assert expected_task_id_attempt in task_id_states_dict
        assert task_id_states_dict[expected_task_id_attempt] == expected_states

    for (
        expected_task_id_attempt,
        expected_error_info,
    ) in expected_task_id_error_info_dict.items():
        assert expected_task_id_attempt in task_id_error_info_dict
        if preserve_proto_field_name:
            assert (
                task_id_error_info_dict[expected_task_id_attempt]["error_type"]
                == expected_error_info["error_type"]
            )
            assert (
                expected_error_info["error_message"]
                in task_id_error_info_dict[expected_task_id_attempt]["error_message"]
            )
        else:
            assert (
                task_id_error_info_dict[expected_task_id_attempt]["errorType"]
                == expected_error_info["errorType"]
            )
            assert (
                expected_error_info["errorMessage"]
                in task_id_error_info_dict[expected_task_id_attempt]["errorMessage"]
            )


def get_and_validate_events(httpserver, validation_func):
    event_data = []
    for http_log in httpserver.log:
        req, _ = http_log
        data = json.loads(req.data)
        event_data.extend(data)

    try:
        validation_func(event_data)
        return True
    except Exception:
        return False


def run_driver_script_and_wait_for_events(script, httpserver, cluster, validation_func):
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)
    node_ids = [node.node_id for node in cluster.list_all_nodes()]
    # Here we wait for the dashboard agent grpc server to be ready before running the
    # driver script. Ideally, the startup sequence should guarantee that. Created an
    # issue to track this: https://github.com/ray-project/ray/issues/58007
    assert wait_until_grpc_channel_ready(cluster.gcs_address, node_ids)
    run_string_as_driver_nonblocking(script)
    wait_for_condition(lambda: get_and_validate_events(httpserver, validation_func))


class TestNormalTaskEvents:
    @_cluster_with_aggregator_target
    def test_normal_task_succeed(
        self,
        ray_start_cluster_head_with_env_vars,
        httpserver,
        preserve_proto_field_name,
    ):
        script = """
import ray
ray.init()

@ray.remote
def normal_task():
    pass
ray.get(normal_task.remote())
    """

        def validate_events(events):
            (
                driver_script_job_id,
                driver_task_id,
            ) = get_job_id_and_driver_script_task_id_from_events(
                events, preserve_proto_field_name
            )

            expected_driver_task_states = {"RUNNING", "FINISHED"}
            expected_normal_task_states = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "SUBMITTED_TO_WORKER",
                "RUNNING",
                "FINISHED",
            }

            # Check definition events
            driver_task_definition_received = False
            normal_task_definition_received = False
            for event in events:
                if preserve_proto_field_name:
                    if event["event_type"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["task_definition_event"]["task_type"] == "DRIVER_TASK":
                            if (
                                event["task_definition_event"]["task_id"]
                                != driver_task_id
                            ):
                                continue
                            driver_task_definition_received = True
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )
                        else:
                            normal_task_definition_received = True
                            normal_task_id = event["task_definition_event"]["task_id"]
                            assert normal_task_id is not None
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["module_name"]
                                == "__main__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["class_name"]
                                == ""
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_name"]
                                == "normal_task"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_hash"]
                                is not None
                            )
                            assert (
                                event["task_definition_event"]["task_name"]
                                == "normal_task"
                            )
                            assert event["task_definition_event"][
                                "required_resources"
                            ] == {"CPU": 1.0}
                            assert (
                                event["task_definition_event"]["job_id"]
                                == driver_script_job_id
                            )
                            assert (
                                event["task_definition_event"]["parent_task_id"]
                                == driver_task_id
                            )
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )
                    else:
                        assert event["event_type"] == "TASK_LIFECYCLE_EVENT"
                else:
                    if event["eventType"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["taskDefinitionEvent"]["taskType"] == "DRIVER_TASK":
                            if event["taskDefinitionEvent"]["taskId"] != driver_task_id:
                                continue
                            driver_task_definition_received = True
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"
                        else:
                            normal_task_definition_received = True
                            normal_task_id = event["taskDefinitionEvent"]["taskId"]
                            assert normal_task_id is not None
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["moduleName"]
                                == "__main__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["className"]
                                == ""
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionName"]
                                == "normal_task"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionHash"]
                                is not None
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskName"]
                                == "normal_task"
                            )
                            assert event["taskDefinitionEvent"][
                                "requiredResources"
                            ] == {"CPU": 1.0}
                            assert (
                                event["taskDefinitionEvent"]["jobId"]
                                == driver_script_job_id
                            )
                            assert (
                                event["taskDefinitionEvent"]["parentTaskId"]
                                == driver_task_id
                            )
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"
                    else:
                        assert event["eventType"] == "TASK_LIFECYCLE_EVENT"

            assert driver_task_definition_received
            assert normal_task_definition_received

            # Check lifecycle events
            expected_task_id_states_dict = {
                (driver_task_id, 0): expected_driver_task_states,
                (normal_task_id, 0): expected_normal_task_states,
            }
            expected_task_id_error_info_dict = {}
            check_task_lifecycle_event_states_and_error_info(
                events,
                expected_task_id_states_dict,
                expected_task_id_error_info_dict,
                preserve_proto_field_name,
            )

        run_driver_script_and_wait_for_events(
            script, httpserver, ray_start_cluster_head_with_env_vars, validate_events
        )

    @_cluster_with_aggregator_target
    def test_normal_task_execution_failure_with_retry(
        self,
        ray_start_cluster_head_with_env_vars,
        httpserver,
        preserve_proto_field_name,
    ):
        script = """
import ray

ray.init()

@ray.remote(max_retries=1, retry_exceptions=[Exception])
def normal_task():
    raise Exception("test error")
try:
    ray.get(normal_task.remote())
except Exception as e:
    pass
        """

        def validate_events(events: json):
            (
                driver_script_job_id,
                driver_task_id,
            ) = get_job_id_and_driver_script_task_id_from_events(
                events, preserve_proto_field_name
            )

            # Check definition events
            driver_task_definition_received = False
            normal_task_definition_received = False
            normal_task_definition_retry_received = False
            for event in events:
                if preserve_proto_field_name:
                    if event["event_type"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["task_definition_event"]["task_type"] == "DRIVER_TASK":
                            if (
                                event["task_definition_event"]["task_id"]
                                != driver_task_id
                            ):
                                continue
                            driver_task_definition_received = True
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )
                        else:
                            normal_task_id = event["task_definition_event"]["task_id"]
                            assert normal_task_id is not None
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["module_name"]
                                == "__main__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["class_name"]
                                == ""
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_name"]
                                == "normal_task"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_hash"]
                                is not None
                            )
                            assert (
                                event["task_definition_event"]["task_name"]
                                == "normal_task"
                            )
                            assert event["task_definition_event"][
                                "required_resources"
                            ] == {"CPU": 1.0}
                            assert (
                                event["task_definition_event"]["job_id"]
                                == driver_script_job_id
                            )
                            assert (
                                event["task_definition_event"]["parent_task_id"]
                                == driver_task_id
                            )
                            if event["task_definition_event"]["task_attempt"] == 0:
                                normal_task_definition_received = True
                            else:
                                assert (
                                    event["task_definition_event"]["task_attempt"] == 1
                                )
                                normal_task_definition_retry_received = True
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )
                    else:
                        assert event["event_type"] == "TASK_LIFECYCLE_EVENT"
                else:
                    if event["eventType"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["taskDefinitionEvent"]["taskType"] == "DRIVER_TASK":
                            if event["taskDefinitionEvent"]["taskId"] != driver_task_id:
                                continue
                            driver_task_definition_received = True
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"
                        else:
                            normal_task_id = event["taskDefinitionEvent"]["taskId"]
                            assert normal_task_id is not None
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["moduleName"]
                                == "__main__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["className"]
                                == ""
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionName"]
                                == "normal_task"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionHash"]
                                is not None
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskName"]
                                == "normal_task"
                            )
                            assert event["taskDefinitionEvent"][
                                "requiredResources"
                            ] == {"CPU": 1.0}
                            assert (
                                event["taskDefinitionEvent"]["jobId"]
                                == driver_script_job_id
                            )
                            assert (
                                event["taskDefinitionEvent"]["parentTaskId"]
                                == driver_task_id
                            )
                            if event["taskDefinitionEvent"]["taskAttempt"] == 0:
                                normal_task_definition_received = True
                            else:
                                assert event["taskDefinitionEvent"]["taskAttempt"] == 1
                                normal_task_definition_retry_received = True
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"
                    else:
                        assert event["eventType"] == "TASK_LIFECYCLE_EVENT"
            assert driver_task_definition_received
            assert normal_task_definition_received
            assert normal_task_definition_retry_received

            # Check execution events
            expected_driver_task_states = {"RUNNING", "FINISHED"}
            expected_normal_task_states = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "SUBMITTED_TO_WORKER",
                "RUNNING",
                "FAILED",
            }
            expected_task_id_states_dict = {
                (driver_task_id, 0): expected_driver_task_states,
                (normal_task_id, 0): expected_normal_task_states,
                (normal_task_id, 1): expected_normal_task_states,
            }
            if preserve_proto_field_name:
                expected_task_id_error_info_dict = {
                    (normal_task_id, 0): {
                        "error_type": "TASK_EXECUTION_EXCEPTION",
                        "error_message": "test error",
                    },
                    (normal_task_id, 1): {
                        "error_type": "TASK_EXECUTION_EXCEPTION",
                        "error_message": "test error",
                    },
                }
            else:
                expected_task_id_error_info_dict = {
                    (normal_task_id, 0): {
                        "errorType": "TASK_EXECUTION_EXCEPTION",
                        "errorMessage": "test error",
                    },
                    (normal_task_id, 1): {
                        "errorType": "TASK_EXECUTION_EXCEPTION",
                        "errorMessage": "test error",
                    },
                }
            check_task_lifecycle_event_states_and_error_info(
                events,
                expected_task_id_states_dict,
                expected_task_id_error_info_dict,
                preserve_proto_field_name,
            )

        run_driver_script_and_wait_for_events(
            script, httpserver, ray_start_cluster_head_with_env_vars, validate_events
        )

    @pytest.mark.skipif(
        True,
        reason="Disabled till https://github.com/ray-project/ray/issues/58016 is fixed",
    )
    @_cluster_with_aggregator_target
    def test_task_failed_due_to_node_failure(
        self,
        ray_start_cluster_head_with_env_vars,
        httpserver,
        preserve_proto_field_name,
    ):
        cluster = ray_start_cluster_head_with_env_vars
        node = cluster.add_node(num_cpus=2)

        script = """
import ray
ray.init()

@ray.remote(num_cpus=2, max_retries=0)
def sleep():
    import time
    time.sleep(999)

x = sleep.options(name="node-killed").remote()
try:
    ray.get(x)
except Exception as e:
    pass
        """
        # Run the driver script and wait for the sleep task to be executing
        def validate_task_running(events: json):
            # Obtain the task id of the sleep task
            normal_task_id = None
            for event in events:
                if preserve_proto_field_name:
                    if (
                        event["event_type"] == "TASK_DEFINITION_EVENT"
                        and event["task_definition_event"]["task_type"] == "NORMAL_TASK"
                    ):
                        normal_task_id = event["task_definition_event"]["task_id"]
                        break
                else:
                    if (
                        event["eventType"] == "TASK_DEFINITION_EVENT"
                        and event["taskDefinitionEvent"]["taskType"] == "NORMAL_TASK"
                    ):
                        normal_task_id = event["taskDefinitionEvent"]["taskId"]
                        break
            assert normal_task_id is not None

            # Check whether the task lifecycle event has running state
            for event in events:
                if preserve_proto_field_name:
                    if (
                        event["event_type"] == "TASK_LIFECYCLE_EVENT"
                        and event["task_lifecycle_event"]["task_id"] == normal_task_id
                    ):
                        for state_transition in event["task_lifecycle_event"][
                            "state_transitions"
                        ]:
                            if state_transition["state"] == "RUNNING":
                                return
                else:
                    if (
                        event["eventType"] == "TASK_LIFECYCLE_EVENT"
                        and event["taskLifecycleEvent"]["taskId"] == normal_task_id
                    ):
                        for state_transition in event["taskLifecycleEvent"][
                            "stateTransitions"
                        ]:
                            if state_transition["state"] == "RUNNING":
                                return
            assert False

        run_driver_script_and_wait_for_events(
            script,
            httpserver,
            ray_start_cluster_head_with_env_vars,
            validate_task_running,
        )

        # Kill the node
        cluster.remove_node(node)

        # Wait and verify the task events
        def validate_task_killed(events: json):
            (
                driver_script_job_id,
                driver_task_id,
            ) = get_job_id_and_driver_script_task_id_from_events(
                events, preserve_proto_field_name
            )

            # Check the task definition events
            driver_task_definition_received = False
            normal_task_definition_received = False
            for event in events:
                if preserve_proto_field_name:
                    if event["event_type"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["task_definition_event"]["task_type"] == "DRIVER_TASK":
                            if (
                                event["task_definition_event"]["task_id"]
                                != driver_task_id
                            ):
                                continue
                            driver_task_definition_received = True
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )
                        else:
                            normal_task_definition_received = True
                            normal_task_id = event["task_definition_event"]["task_id"]
                            assert normal_task_id is not None
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["module_name"]
                                == "__main__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["class_name"]
                                == ""
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_name"]
                                == "sleep"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_hash"]
                                is not None
                            )
                            assert (
                                event["task_definition_event"]["task_name"]
                                == "node-killed"
                            )
                            assert event["task_definition_event"][
                                "required_resources"
                            ] == {"CPU": 2.0}
                            assert (
                                event["task_definition_event"]["job_id"]
                                == driver_script_job_id
                            )
                            assert (
                                event["task_definition_event"]["parent_task_id"]
                                == driver_task_id
                            )
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )
                    else:
                        assert event["event_type"] == "TASK_LIFECYCLE_EVENT"
                else:
                    if event["eventType"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["taskDefinitionEvent"]["taskType"] == "DRIVER_TASK":
                            if event["taskDefinitionEvent"]["taskId"] != driver_task_id:
                                continue
                            driver_task_definition_received = True
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"
                        else:
                            normal_task_definition_received = True
                            normal_task_id = event["taskDefinitionEvent"]["taskId"]
                            assert normal_task_id is not None
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["moduleName"]
                                == "__main__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["className"]
                                == ""
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionName"]
                                == "sleep"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionHash"]
                                is not None
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskName"]
                                == "node-killed"
                            )
                            assert event["taskDefinitionEvent"][
                                "requiredResources"
                            ] == {"CPU": 2.0}
                            assert (
                                event["taskDefinitionEvent"]["jobId"]
                                == driver_script_job_id
                            )
                            assert (
                                event["taskDefinitionEvent"]["parentTaskId"]
                                == driver_task_id
                            )
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"
                    else:
                        assert event["eventType"] == "TASK_LIFECYCLE_EVENT"
            assert driver_task_definition_received
            assert normal_task_definition_received

            # Check the task lifecycle events
            expected_driver_task_states = {"RUNNING", "FINISHED"}
            expected_normal_task_states = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "SUBMITTED_TO_WORKER",
                "RUNNING",
                "FAILED",
            }
            expected_task_id_states_dict = {
                (driver_task_id, 0): expected_driver_task_states,
                (normal_task_id, 0): expected_normal_task_states,
            }
            if preserve_proto_field_name:
                expected_task_id_error_info_dict = {
                    (normal_task_id, 0): {
                        "error_type": "NODE_DIED",
                        "error_message": "Task failed due to the node (where this task was running)  was dead or unavailable",
                    }
                }
            else:
                expected_task_id_error_info_dict = {
                    (normal_task_id, 0): {
                        "errorType": "NODE_DIED",
                        "errorMessage": "Task failed due to the node (where this task was running)  was dead or unavailable",
                    }
                }
            check_task_lifecycle_event_states_and_error_info(
                events,
                expected_task_id_states_dict,
                expected_task_id_error_info_dict,
                preserve_proto_field_name,
            )

        wait_for_condition(
            lambda: get_and_validate_events(httpserver, validate_task_killed),
        )


class TestActorTaskEvents:
    @_cluster_with_aggregator_target
    def test_actor_creation_succeed(
        self,
        ray_start_cluster_head_with_env_vars,
        httpserver,
        preserve_proto_field_name,
    ):
        script = """
import ray
ray.init()

@ray.remote(num_cpus=1)
class Actor:
    def __init__(self):
        pass

    def task(self, arg):
        pass

actor = Actor.remote()
obj = ray.put("test")
ray.get(actor.task.remote(obj))
        """

        def validate_events(events: json):
            (
                driver_script_job_id,
                driver_task_id,
            ) = get_job_id_and_driver_script_task_id_from_events(
                events, preserve_proto_field_name
            )

            driver_task_definition_received = False
            actor_creation_task_definition_received = False
            actor_task_definition_received = False
            for event in events:
                if preserve_proto_field_name:
                    if event["event_type"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["task_definition_event"]["task_type"] == "DRIVER_TASK":
                            driver_task_definition_received = True
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )

                        else:
                            assert (
                                event["task_definition_event"]["task_type"]
                                == "ACTOR_CREATION_TASK"
                            )
                            actor_creation_task_definition_received = True
                            actor_creation_task_id = event["task_definition_event"][
                                "task_id"
                            ]
                            assert actor_creation_task_id is not None
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["module_name"]
                                == "__main__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["class_name"]
                                == "Actor"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_name"]
                                == "__init__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_hash"]
                                is not None
                            )
                            assert (
                                event["task_definition_event"]["task_name"]
                                == "Actor.__init__"
                            )
                            assert event["task_definition_event"][
                                "required_resources"
                            ] == {"CPU": 1.0}
                            assert (
                                event["task_definition_event"]["parent_task_id"]
                                == driver_task_id
                            )
                            assert (
                                event["task_definition_event"]["job_id"]
                                == driver_script_job_id
                            )
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )

                    elif event["event_type"] == "ACTOR_TASK_DEFINITION_EVENT":
                        actor_task_definition_received = True
                        actor_task_id = event["actor_task_definition_event"]["task_id"]
                        assert actor_task_id is not None
                        assert (
                            event["actor_task_definition_event"]["actor_func"][
                                "python_function_descriptor"
                            ]["module_name"]
                            == "__main__"
                        )
                        assert (
                            event["actor_task_definition_event"]["actor_func"][
                                "python_function_descriptor"
                            ]["class_name"]
                            == "Actor"
                        )
                        assert (
                            event["actor_task_definition_event"]["actor_func"][
                                "python_function_descriptor"
                            ]["function_name"]
                            == "task"
                        )
                        assert (
                            event["actor_task_definition_event"]["actor_func"][
                                "python_function_descriptor"
                            ]["function_hash"]
                            is not None
                        )
                        assert (
                            event["actor_task_definition_event"]["actor_task_name"]
                            == "Actor.task"
                        )
                        assert (
                            event["actor_task_definition_event"]["required_resources"]
                            == {}
                        )
                        assert (
                            event["actor_task_definition_event"]["job_id"]
                            == driver_script_job_id
                        )
                        assert (
                            event["actor_task_definition_event"]["parent_task_id"]
                            == driver_task_id
                        )
                        assert event["actor_task_definition_event"]["task_attempt"] == 0
                        assert (
                            event["actor_task_definition_event"]["language"] == "PYTHON"
                        )

                    else:
                        assert event["event_type"] == "TASK_LIFECYCLE_EVENT"
                else:
                    if event["eventType"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["taskDefinitionEvent"]["taskType"] == "DRIVER_TASK":
                            driver_task_definition_received = True
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"

                        else:
                            assert (
                                event["taskDefinitionEvent"]["taskType"]
                                == "ACTOR_CREATION_TASK"
                            )
                            actor_creation_task_definition_received = True
                            actor_creation_task_id = event["taskDefinitionEvent"][
                                "taskId"
                            ]
                            assert actor_creation_task_id is not None
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["moduleName"]
                                == "__main__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["className"]
                                == "Actor"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionName"]
                                == "__init__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionHash"]
                                is not None
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskName"]
                                == "Actor.__init__"
                            )
                            assert event["taskDefinitionEvent"][
                                "requiredResources"
                            ] == {"CPU": 1.0}
                            assert (
                                event["taskDefinitionEvent"]["parentTaskId"]
                                == driver_task_id
                            )
                            assert (
                                event["taskDefinitionEvent"]["jobId"]
                                == driver_script_job_id
                            )
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"

                    elif event["eventType"] == "ACTOR_TASK_DEFINITION_EVENT":
                        actor_task_definition_received = True
                        actor_task_id = event["actorTaskDefinitionEvent"]["taskId"]
                        assert actor_task_id is not None
                        assert (
                            event["actorTaskDefinitionEvent"]["actorFunc"][
                                "pythonFunctionDescriptor"
                            ]["moduleName"]
                            == "__main__"
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["actorFunc"][
                                "pythonFunctionDescriptor"
                            ]["className"]
                            == "Actor"
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["actorFunc"][
                                "pythonFunctionDescriptor"
                            ]["functionName"]
                            == "task"
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["actorFunc"][
                                "pythonFunctionDescriptor"
                            ]["functionHash"]
                            is not None
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["actorTaskName"]
                            == "Actor.task"
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["requiredResources"] == {}
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["jobId"]
                            == driver_script_job_id
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["parentTaskId"]
                            == driver_task_id
                        )
                        assert event["actorTaskDefinitionEvent"]["taskAttempt"] == 0
                        assert event["actorTaskDefinitionEvent"]["language"] == "PYTHON"

                    else:
                        assert event["eventType"] == "TASK_LIFECYCLE_EVENT"

            assert driver_task_definition_received
            assert actor_creation_task_definition_received
            assert actor_task_definition_received

            expected_driver_task_states = {"RUNNING", "FINISHED"}
            expected_actor_creation_task_states = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "RUNNING",
                "FINISHED",
            }
            expected_actor_task_states = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "SUBMITTED_TO_WORKER",
                "PENDING_ACTOR_TASK_ARGS_FETCH",
                "PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY",
                "RUNNING",
                "FINISHED",
            }
            expected_task_id_states_dict = {
                (driver_task_id, 0): expected_driver_task_states,
                (actor_creation_task_id, 0): expected_actor_creation_task_states,
                (actor_task_id, 0): expected_actor_task_states,
            }
            expected_task_id_error_info_dict = {}
            check_task_lifecycle_event_states_and_error_info(
                events,
                expected_task_id_states_dict,
                expected_task_id_error_info_dict,
                preserve_proto_field_name,
            )

        run_driver_script_and_wait_for_events(
            script, httpserver, ray_start_cluster_head_with_env_vars, validate_events
        )

    @_cluster_with_aggregator_target
    def test_actor_creation_failed(
        self,
        ray_start_cluster_head_with_env_vars,
        httpserver,
        preserve_proto_field_name,
    ):
        script = """
import ray
import ray.util.state
from ray._common.test_utils import wait_for_condition
import time

@ray.remote(num_cpus=1)
class Actor:
    def __init__(self):
        time.sleep(1)
        raise Exception("actor creation error")

    def task(self):
        pass

actor = Actor.remote()
wait_for_condition(lambda: ray.util.state.list_actors(filters=[("class_name", "=", "Actor")])[0]["state"] == "DEAD")
ray.get(actor.task.options().remote())
        """

        def validate_events(events: json):
            (
                driver_script_job_id,
                driver_task_id,
            ) = get_job_id_and_driver_script_task_id_from_events(
                events, preserve_proto_field_name
            )

            driver_task_definition_received = False
            actor_creation_task_definition_received = False
            actor_task_definition_received = False
            for event in events:
                if preserve_proto_field_name:
                    if event["event_type"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["task_definition_event"]["task_type"] == "DRIVER_TASK":
                            driver_task_definition_received = True
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )

                        else:
                            assert (
                                event["task_definition_event"]["task_type"]
                                == "ACTOR_CREATION_TASK"
                            )
                            actor_creation_task_definition_received = True
                            actor_creation_task_id = event["task_definition_event"][
                                "task_id"
                            ]
                            assert actor_creation_task_id is not None
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["module_name"]
                                == "__main__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["class_name"]
                                == "Actor"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_name"]
                                == "__init__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_hash"]
                                is not None
                            )
                            assert (
                                event["task_definition_event"]["task_name"]
                                == "Actor.__init__"
                            )
                            assert event["task_definition_event"][
                                "required_resources"
                            ] == {"CPU": 1.0}
                            assert (
                                event["task_definition_event"]["parent_task_id"]
                                == driver_task_id
                            )
                            assert (
                                event["task_definition_event"]["job_id"]
                                == driver_script_job_id
                            )
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )
                    elif event["event_type"] == "ACTOR_TASK_DEFINITION_EVENT":
                        actor_task_definition_received = True
                        actor_task_id = event["actor_task_definition_event"]["task_id"]
                        assert actor_task_id is not None
                        assert (
                            event["actor_task_definition_event"]["actor_func"][
                                "python_function_descriptor"
                            ]["module_name"]
                            == "__main__"
                        )
                        assert (
                            event["actor_task_definition_event"]["actor_func"][
                                "python_function_descriptor"
                            ]["class_name"]
                            == "Actor"
                        )
                        assert (
                            event["actor_task_definition_event"]["actor_func"][
                                "python_function_descriptor"
                            ]["function_name"]
                            == "task"
                        )
                        assert (
                            event["actor_task_definition_event"]["actor_func"][
                                "python_function_descriptor"
                            ]["function_hash"]
                            is not None
                        )
                        assert (
                            event["actor_task_definition_event"]["actor_task_name"]
                            == "Actor.task"
                        )
                        assert (
                            event["actor_task_definition_event"]["required_resources"]
                            == {}
                        )
                        assert (
                            event["actor_task_definition_event"]["job_id"]
                            == driver_script_job_id
                        )
                        assert (
                            event["actor_task_definition_event"]["parent_task_id"]
                            == driver_task_id
                        )
                        assert event["actor_task_definition_event"]["task_attempt"] == 0
                        assert (
                            event["actor_task_definition_event"]["language"] == "PYTHON"
                        )
                    else:
                        assert event["event_type"] == "TASK_LIFECYCLE_EVENT"
                else:
                    if event["eventType"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["taskDefinitionEvent"]["taskType"] == "DRIVER_TASK":
                            driver_task_definition_received = True
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"

                        else:
                            assert (
                                event["taskDefinitionEvent"]["taskType"]
                                == "ACTOR_CREATION_TASK"
                            )
                            actor_creation_task_definition_received = True
                            actor_creation_task_id = event["taskDefinitionEvent"][
                                "taskId"
                            ]
                            assert actor_creation_task_id is not None
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["moduleName"]
                                == "__main__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["className"]
                                == "Actor"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionName"]
                                == "__init__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionHash"]
                                is not None
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskName"]
                                == "Actor.__init__"
                            )
                            assert event["taskDefinitionEvent"][
                                "requiredResources"
                            ] == {"CPU": 1.0}
                            assert (
                                event["taskDefinitionEvent"]["parentTaskId"]
                                == driver_task_id
                            )
                            assert (
                                event["taskDefinitionEvent"]["jobId"]
                                == driver_script_job_id
                            )
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"
                    elif event["eventType"] == "ACTOR_TASK_DEFINITION_EVENT":
                        actor_task_definition_received = True
                        actor_task_id = event["actorTaskDefinitionEvent"]["taskId"]
                        assert actor_task_id is not None
                        assert (
                            event["actorTaskDefinitionEvent"]["actorFunc"][
                                "pythonFunctionDescriptor"
                            ]["moduleName"]
                            == "__main__"
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["actorFunc"][
                                "pythonFunctionDescriptor"
                            ]["className"]
                            == "Actor"
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["actorFunc"][
                                "pythonFunctionDescriptor"
                            ]["functionName"]
                            == "task"
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["actorFunc"][
                                "pythonFunctionDescriptor"
                            ]["functionHash"]
                            is not None
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["actorTaskName"]
                            == "Actor.task"
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["requiredResources"] == {}
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["jobId"]
                            == driver_script_job_id
                        )
                        assert (
                            event["actorTaskDefinitionEvent"]["parentTaskId"]
                            == driver_task_id
                        )
                        assert event["actorTaskDefinitionEvent"]["taskAttempt"] == 0
                        assert event["actorTaskDefinitionEvent"]["language"] == "PYTHON"
                    else:
                        assert event["eventType"] == "TASK_LIFECYCLE_EVENT"
            assert driver_task_definition_received
            assert actor_creation_task_definition_received
            assert actor_task_definition_received

            expected_driver_task_states = {"RUNNING", "FINISHED"}
            expected_actor_creation_task_states = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "RUNNING",
                "FAILED",
            }
            expected_actor_task_states = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "FAILED",
            }
            expected_task_id_states_dict = {
                (driver_task_id, 0): expected_driver_task_states,
                (actor_creation_task_id, 0): expected_actor_creation_task_states,
                (actor_task_id, 0): expected_actor_task_states,
            }
            if preserve_proto_field_name:
                expected_task_id_error_info_dict = {
                    (actor_creation_task_id, 0): {
                        "error_type": "TASK_EXECUTION_EXCEPTION",
                        "error_message": "CreationTaskError: Exception raised from an actor init method.",
                    },
                    (actor_task_id, 0): {
                        "error_type": "ACTOR_DIED",
                        "error_message": "ray.exceptions.ActorDiedError: The actor died because of an error raised in its creation task",
                    },
                }
            else:
                expected_task_id_error_info_dict = {
                    (actor_creation_task_id, 0): {
                        "errorType": "TASK_EXECUTION_EXCEPTION",
                        "errorMessage": "CreationTaskError: Exception raised from an actor init method.",
                    },
                    (actor_task_id, 0): {
                        "errorType": "ACTOR_DIED",
                        "errorMessage": "ray.exceptions.ActorDiedError: The actor died because of an error raised in its creation task",
                    },
                }
            check_task_lifecycle_event_states_and_error_info(
                events,
                expected_task_id_states_dict,
                expected_task_id_error_info_dict,
                preserve_proto_field_name,
            )

        run_driver_script_and_wait_for_events(
            script, httpserver, ray_start_cluster_head_with_env_vars, validate_events
        )

    @_cluster_with_aggregator_target
    def test_actor_creation_canceled(
        self,
        ray_start_cluster_head_with_env_vars,
        httpserver,
        preserve_proto_field_name,
    ):
        script = """
import ray
ray.init()

@ray.remote(num_cpus=2)
class Actor:
    def __init__(self):
        pass

    def task(self):
        pass

actor = Actor.remote()
ray.kill(actor)
        """

        def validate_events(events: json):
            (
                driver_script_job_id,
                driver_task_id,
            ) = get_job_id_and_driver_script_task_id_from_events(
                events, preserve_proto_field_name
            )

            driver_task_definition_received = False
            actor_creation_task_definition_received = False
            for event in events:
                if preserve_proto_field_name:
                    if event["event_type"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["task_definition_event"]["task_type"] == "DRIVER_TASK":
                            driver_task_definition_received = True
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )

                        else:
                            assert (
                                event["task_definition_event"]["task_type"]
                                == "ACTOR_CREATION_TASK"
                            )
                            actor_creation_task_definition_received = True
                            actor_creation_task_id = event["task_definition_event"][
                                "task_id"
                            ]
                            assert actor_creation_task_id is not None
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["module_name"]
                                == "__main__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["class_name"]
                                == "Actor"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_name"]
                                == "__init__"
                            )
                            assert (
                                event["task_definition_event"]["task_func"][
                                    "python_function_descriptor"
                                ]["function_hash"]
                                is not None
                            )
                            assert (
                                event["task_definition_event"]["task_name"]
                                == "Actor.__init__"
                            )
                            assert event["task_definition_event"][
                                "required_resources"
                            ] == {"CPU": 2.0}
                            assert (
                                event["task_definition_event"]["parent_task_id"]
                                == driver_task_id
                            )
                            assert (
                                event["task_definition_event"]["job_id"]
                                == driver_script_job_id
                            )
                            assert event["task_definition_event"]["task_attempt"] == 0
                            assert (
                                event["task_definition_event"]["language"] == "PYTHON"
                            )
                    else:
                        assert event["event_type"] == "TASK_LIFECYCLE_EVENT"
                else:
                    if event["eventType"] == "TASK_DEFINITION_EVENT":
                        check_task_event_base_fields(event, preserve_proto_field_name)

                        if event["taskDefinitionEvent"]["taskType"] == "DRIVER_TASK":
                            driver_task_definition_received = True
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"

                        else:
                            assert (
                                event["taskDefinitionEvent"]["taskType"]
                                == "ACTOR_CREATION_TASK"
                            )
                            actor_creation_task_definition_received = True
                            actor_creation_task_id = event["taskDefinitionEvent"][
                                "taskId"
                            ]
                            assert actor_creation_task_id is not None
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["moduleName"]
                                == "__main__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["className"]
                                == "Actor"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionName"]
                                == "__init__"
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskFunc"][
                                    "pythonFunctionDescriptor"
                                ]["functionHash"]
                                is not None
                            )
                            assert (
                                event["taskDefinitionEvent"]["taskName"]
                                == "Actor.__init__"
                            )
                            assert event["taskDefinitionEvent"][
                                "requiredResources"
                            ] == {"CPU": 2.0}
                            assert (
                                event["taskDefinitionEvent"]["parentTaskId"]
                                == driver_task_id
                            )
                            assert (
                                event["taskDefinitionEvent"]["jobId"]
                                == driver_script_job_id
                            )
                            assert event["taskDefinitionEvent"]["taskAttempt"] == 0
                            assert event["taskDefinitionEvent"]["language"] == "PYTHON"
                    else:
                        assert event["eventType"] == "TASK_LIFECYCLE_EVENT"

            assert driver_task_definition_received
            assert actor_creation_task_definition_received

            expected_driver_task_states = {"RUNNING", "FINISHED"}
            expected_actor_creation_task_states = {
                "PENDING_ARGS_AVAIL",
                "PENDING_NODE_ASSIGNMENT",
                "FAILED",
            }
            expected_task_id_states_dict = {
                (driver_task_id, 0): expected_driver_task_states,
                (actor_creation_task_id, 0): expected_actor_creation_task_states,
            }
            if preserve_proto_field_name:
                expected_task_id_error_info_dict = {
                    (actor_creation_task_id, 0): {
                        "error_type": "WORKER_DIED",
                        "error_message": "",
                    }
                }
            else:
                expected_task_id_error_info_dict = {
                    (actor_creation_task_id, 0): {
                        "errorType": "WORKER_DIED",
                        "errorMessage": "",
                    }
                }
            check_task_lifecycle_event_states_and_error_info(
                events,
                expected_task_id_states_dict,
                expected_task_id_error_info_dict,
                preserve_proto_field_name,
            )

        run_driver_script_and_wait_for_events(
            script, httpserver, ray_start_cluster_head_with_env_vars, validate_events
        )


if __name__ == "__main__":
    pytest.main(["-vv", __file__])

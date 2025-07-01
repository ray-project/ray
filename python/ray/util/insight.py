import ray
import threading
import os
import uuid
import time
import asyncio
import socket
from contextlib import contextmanager
import ray._private
from ray.experimental import internal_kv
import ray.dashboard.consts as dashboard_consts
from flow_insight import (
    UsageModel,
    StorageType,
    InsightClient,
    FastAPIInsightServer,
    CallSubmitEvent,
    CallBeginEvent,
    CallEndEvent,
    ObjectGetEvent,
    ObjectPutEvent,
    ContextEvent,
    ResourceUsageEvent,
    DebuggerInfoEvent,
)

_insight_client = None


def create_http_insight_client(insight_server_address: str):
    return InsightClient(
        server_url=f"http://{insight_server_address}",
    )


def insight_server_is_alive():
    if _insight_client is None:
        return False
    resp = _insight_client.ping()
    if not resp["result"]:
        return False
    return True


def get_insight_client():
    global _insight_client
    if _insight_client is None or not insight_server_is_alive():
        address = None
        while address is None:
            address = internal_kv._internal_kv_get(
                "insight_monitor_address", namespace="flowinsight"
            )
            if address is None:
                time.sleep(1)

        _insight_client = create_http_insight_client(address.decode())
    return _insight_client


def get_current_worker_id():
    """
    Get the current worker ID.
    """
    return ray._private.worker.global_worker.worker_id


def get_current_job_id():
    """
    Get the current job ID.
    """
    return ray._private.worker.global_worker.current_job_id.hex()


def create_insight_monitor_actor():
    if not is_flow_insight_enabled():
        return
    try:
        ray.get_actor("_ray_internal_insight_monitor", namespace="flowinsight")
    except ValueError:
        _ray_internal_insight_monitor.options(
            name="_ray_internal_insight_monitor",
            namespace="flowinsight",
            lifetime="detached",
        ).remote()


@ray.remote(max_restarts=-1)
class _ray_internal_insight_monitor:
    def __init__(self):
        self.node_ip_address = ray._private.services.get_node_ip_address()
        self.port = self._get_free_port()
        print(f"Starting insight monitor on {self.node_ip_address}:{self.port}")
        session_id = ray._private.worker._global_node.session_name
        self.server = FastAPIInsightServer(
            snapshot_storage_type=StorageType.MEMORY,
            snapshot_duration_s=600,
            storage_dir=os.path.join(
                ray._private.utils.get_ray_temp_dir(), session_id, "flowinsight"
            ),
        )

        def run_server():
            asyncio.run(self.server.run(host=self.node_ip_address, port=self.port))

        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()

        # Save address in KV store using _internal_kv_put
        internal_kv._internal_kv_put(
            "insight_monitor_address",
            f"{self.node_ip_address}:{self.port}".encode(),
            namespace="flowinsight",
        )

    def _get_free_port(self):
        """Get a free port on the current node."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            s.listen(1)
            port = s.getsockname()[1]
        return port


_inner_class_name = "_ray_internal_insight_monitor"
_null_object_id = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"


def _get_current_task_name():
    if ray._private.worker.global_worker.mode == ray._private.worker.WORKER_MODE:
        current_task_name = ray._private.worker.global_worker.current_task_name
        if current_task_name is not None:
            return current_task_name.split(".")[-1]
    return "_main"


def get_current_task_id():
    try:
        current_task_id = ray._private.worker.global_worker.current_task_id
        if current_task_id.is_nil():
            current_task_id = "_main"
        else:
            current_task_id = current_task_id.hex()
    except:
        current_task_id = "_main"
    return current_task_id


def _get_caller_class():
    caller_class = None
    try:
        # caller actor can be fetched from the runtime context
        # but it may raise Exception if called in the driver or in a task
        actor_id = ray._private.worker.global_worker.actor_id
        if actor_id.is_nil():
            return None, None
        caller_actor = ray._private.worker.global_worker.core_worker.get_actor_handle(
            actor_id
        )
        if caller_actor is not None:
            actor_name = ray._private.worker.global_worker.actor_name
            if actor_name is not None and actor_name != "":
                return actor_name, actor_id.hex()
            return (
                caller_actor._ray_actor_creation_function_descriptor.class_name.split(
                    "."
                )[-1],
                caller_actor._ray_actor_id.hex(),
            )
    except Exception:
        pass

    return caller_class


def is_visual_rdb_enabled():
    """
    Check if visual debug is enabled.
    """
    return os.environ.get("RAY_VISUAL_RDB", "0") == "1"


def is_flow_insight_enabled():
    """
    Check if flow insight is enabled.
    """
    return os.getenv(dashboard_consts.FLOW_INSIGHT_ENABLED_ENV_VAR, "0") == "1"


def need_record(caller_class):
    if (
        caller_class is not None
        and caller_class[0] is not None
        and caller_class[0].startswith(_inner_class_name)
    ):
        return False
    return True


def record_control_flow(callee_class, callee_func):
    """
    record the control flow between the caller and the callee
    this will get caller context automatically from the runtime context

    param:
        callee_class: the class name of the callee
        callee_func: the function name of the callee
    """
    if not is_flow_insight_enabled():
        return

    if not need_record(callee_class):
        return

    try:
        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()
        current_task_id = get_current_task_id()

        # Create a record for this call
        job_id = get_current_job_id()

        get_insight_client().emit_event(
            CallSubmitEvent(
                flow_id=job_id,
                source_service=caller_class[0],
                source_instance_id=caller_class[1],
                source_method=caller_func,
                target_service=None if callee_class is None else callee_class[0],
                target_instance_id=None if callee_class is None else callee_class[1],
                target_method=callee_func,
                timestamp=int(time.time() * 1000),
                parent_span_id=current_task_id,
            )
        )

    except Exception as e:
        print(f"Error recording control flow: {e}")


def record_object_arg_get(object_id):
    """
    record the object get event for the task's args
    this will get caller context automatically from the runtime context

    param:
        object_id: the object id of the task's args
    """
    if not is_flow_insight_enabled():
        return

    if object_id is None or object_id == _null_object_id:
        return

    try:
        caller_class = _get_caller_class()

        if not need_record(caller_class):
            return

        recv_func = _get_current_task_name()

        job_id = get_current_job_id()

        get_insight_client().emit_event(
            ObjectGetEvent(
                flow_id=job_id,
                object_id=object_id,
                receiver_service=caller_class[0],
                receiver_instance_id=caller_class[1],
                receiver_method=recv_func,
                timestamp=int(time.time() * 1000),
            )
        )

    except Exception as e:
        print(f"Error recording object arg get: {e}")


def record_object_put(object_id, size):
    """
    record the object put event for a general ray.put
    this will get caller context automatically from the runtime context

    param:
        object_id: the object id of the object to be put
        size: the size of the object to be put
    """
    if not is_flow_insight_enabled():
        return

    if object_id == _null_object_id:
        return

    try:
        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()

        if not need_record(caller_class):
            return

        # Create a record for this call
        job_id = get_current_job_id()

        get_insight_client().emit_event(
            ObjectPutEvent(
                flow_id=job_id,
                object_id=object_id,
                object_size=size,
                object_pos=-2,
                sender_service=caller_class[0],
                sender_instance_id=caller_class[1],
                sender_method=caller_func,
                timestamp=int(time.time() * 1000),
            )
        )

    except Exception as e:
        print(f"Error recording object put: {e}")


def record_object_arg_put(object_id, argpos, size, callee):
    """
    record the object put event for the task's args
    this will get caller context automatically from the runtime context
    callee is used to prevent recursive call for monitor actor

    param:
        object_id: the object id of the task's args
        size: the size of the task's args
        callee: the callee function info, e.g. "ActorClass.method_name"
    """
    if not is_flow_insight_enabled():
        return

    if object_id == _null_object_id:
        return

    try:
        callee_class = None
        callee_info = callee.split(".")
        if len(callee_info) == 2:
            callee_class = None
        elif len(callee_info) == 3:
            callee_class = callee_info[-2]

        if not need_record(callee_class):
            return

        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()
        # Create a record for this call
        job_id = get_current_job_id()

        get_insight_client().emit_event(
            ObjectPutEvent(
                flow_id=job_id,
                object_id=object_id,
                object_size=size,
                object_pos=argpos,
                sender_service=caller_class[0],
                sender_instance_id=caller_class[1],
                sender_method=caller_func,
                timestamp=int(time.time() * 1000),
            )
        )

    except Exception as e:
        print(f"Error recording object arg put: {e}")


def record_object_return_put(object_id, size):
    """
    record the object put event for the task's return value
    this will get caller context automatically from the runtime context

    param:
        object_id: the object id of the task's return value
        size: the size of the task's return value
    """
    if not is_flow_insight_enabled():
        return

    if object_id == _null_object_id:
        return

    if size == 0:
        return

    try:
        caller_class = _get_caller_class()

        if not need_record(caller_class):
            return

        caller_func = _get_current_task_name()

        job_id = get_current_job_id()

        get_insight_client().emit_event(
            ObjectPutEvent(
                flow_id=job_id,
                object_id=object_id,
                object_size=size,
                object_pos=-1,
                sender_service=caller_class[0],
                sender_instance_id=caller_class[1],
                sender_method=caller_func,
                timestamp=int(time.time() * 1000),
            )
        )

    except Exception as e:
        print(f"Error recording object return put: {e}")


def record_object_get(object_id, task_id):
    """
    record the object get event for a general ray.get
    this will get caller context automatically from the runtime context
    task_id is used to prevent recursive call for monitor actor
    since we can get callee actor id from the task_id

    param:
        object_id: the object id of the object to be get
        task_id: the task id of the task to be get
    """
    if not is_flow_insight_enabled():
        return

    if object_id is None or object_id == _null_object_id:
        return

    try:
        # Get the task name from the runtime context
        # if there is no task name, it should be the driver
        recv_func = _get_current_task_name()
        caller_class = _get_caller_class()

        job_id = get_current_job_id()

        if not need_record(caller_class):
            return

        get_insight_client().emit_event(
            ObjectGetEvent(
                flow_id=job_id,
                object_id=object_id,
                receiver_service=caller_class[0],
                receiver_instance_id=caller_class[1],
                receiver_method=recv_func,
                timestamp=int(time.time() * 1000),
            )
        )

    except Exception as e:
        print(f"Error recording object get: {e}")


def report_resource_usage(usage: dict):
    """
    report the resource usage of the current task
    usage is a dict of the resource usage
    e.g. {"torch_gram": {"used": 1024, "base": "gpu"}}
    """
    if not is_flow_insight_enabled():
        return

    try:
        current_class = _get_caller_class()
        if current_class is None:
            return

        job_id = get_current_job_id()

        if not need_record(current_class):
            return

        for key, value in usage.items():
            usage[key] = UsageModel(
                used=value["used"],
                base=value["base"],
            )

        get_insight_client().emit_event(
            ResourceUsageEvent(
                flow_id=job_id,
                service_name=current_class[0],
                instance_id=current_class[1],
                method_name=_get_current_task_name(),
                usage=usage,
                timestamp=int(time.time() * 1000),
            )
        )

    except Exception as e:
        print(f"Error reporting resource usage: {e}")


def register_current_context(context_data: dict):
    """
    register the current context info of the current node
    """
    if not is_flow_insight_enabled():
        return

    try:
        current_class = _get_caller_class()
        if current_class is None:
            return

        job_id = get_current_job_id()

        if not need_record(current_class):
            return

        get_insight_client().emit_event(
            ContextEvent(
                flow_id=job_id,
                service_name=current_class[0],
                instance_id=current_class[1],
                method_name=_get_current_task_name(),
                context=context_data,
                timestamp=int(time.time() * 1000),
            )
        )

    except Exception as e:
        print(f"Error registering current context: {e}")


def report_torch_gram():
    """
    report the torch gram usage of the current task
    """
    if not is_flow_insight_enabled():
        return

    try:
        import torch
    except ImportError:
        return

    try:
        report_resource_usage(
            {
                "torch_gram_allocated": {
                    "used": torch.cuda.memory_allocated() / 1024 / 1024,
                    "base": "gpu",
                },
                "torch_gram_max_allocated": {
                    "used": torch.cuda.max_memory_allocated() / 1024 / 1024,
                    "base": "gpu",
                },
            }
        )
    except Exception as e:
        print(f"Error reporting torch gram: {e}")


def record_task_duration(duration):
    """
    Record the duration of a task execution for flame graph visualization.
    This should be called at the end of a task or actor method.
    """
    if not is_flow_insight_enabled():
        return

    if duration is None:
        return

    try:
        caller_class = _get_caller_class()
        caller_func = _get_current_task_name()

        if not need_record(caller_class):
            return

        current_task_id = get_current_task_id()

        job_id = get_current_job_id()

        get_insight_client().emit_event(
            CallEndEvent(
                flow_id=job_id,
                target_service=caller_class[0],
                target_instance_id=caller_class[1],
                target_method=caller_func,
                duration=duration,
                span_id=current_task_id,
                timestamp=int(time.time() * 1000),
            )
        )

    except Exception as e:
        print(f"Error recording task duration: {e}")


class profile:
    def __init__(self, func_name, group_name=""):
        if group_name != "":
            self.func_name = f"{func_name}/{group_name}"
        else:
            self.func_name = func_name
        self.job_id = None
        self.start_time = None
        self.span_id = None
        self._start()

    def _start(self):
        if not is_flow_insight_enabled():
            return

        if not hasattr(ray._private.worker.global_worker, "_insight_call_stack"):
            setattr(ray._private.worker.global_worker, "_insight_call_stack", [])

        if len(ray._private.worker.global_worker._insight_call_stack) > 0:
            frame = ray._private.worker.global_worker._insight_call_stack[-1]
            current_task_id = frame["parent_span_id"]
            caller_class = frame["caller_class"]
            caller_func = frame["caller_func"]
        else:
            current_task_id = get_current_task_id()
            caller_class = _get_caller_class()
            caller_func = _get_current_task_name()

        if not need_record(caller_class):
            return

        job_id = get_current_job_id()
        self.job_id = job_id
        span_id = str(uuid.uuid4())
        self.span_id = span_id

        self.start_time = time.time()
        get_insight_client().emit_event(
            CallSubmitEvent(
                flow_id=job_id,
                source_service=caller_class[0],
                source_instance_id=caller_class[1],
                source_method=caller_func,
                target_service=None,
                target_instance_id=None,
                target_method=self.func_name,
                timestamp=int(time.time() * 1000),
                parent_span_id=current_task_id,
            )
        )

        get_insight_client().emit_event(
            CallBeginEvent(
                flow_id=job_id,
                source_service=None if caller_class is None else caller_class[0],
                source_instance_id=None if caller_class is None else caller_class[1],
                source_method=caller_func,
                target_service=None,
                target_instance_id=None,
                target_method=self.func_name,
                parent_span_id=current_task_id,
                span_id=span_id,
                timestamp=int(time.time() * 1000),
            )
        )
        frame = {
            "parent_span_id": span_id,
            "caller_class": (None, None),
            "caller_func": self.func_name,
        }
        ray._private.worker.global_worker._insight_call_stack.append(frame)

    def end(self):
        get_insight_client().emit_event(
            CallEndEvent(
                flow_id=self.job_id,
                target_service=None,
                target_instance_id=None,
                target_method=self.func_name,
                duration=time.time() - self.start_time,
                span_id=self.span_id,
                timestamp=int(time.time() * 1000),
            )
        )
        if len(ray._private.worker.global_worker._insight_call_stack) > 0:
            ray._private.worker.global_worker._insight_call_stack.pop()


@contextmanager
def timeit(func_name, group_name=""):
    """A context manager for recording task execution timing in Ray.

    This context manager automatically records the start and end time of a task
    for flame graph visualization. It should be used within Ray tasks or actor methods.

    Example:
        @ray.remote
        def my_task():
            with timeit():
                # Your task code here
                result = do_work()
                return result

        @ray.remote
        class MyActor:
            def my_method(self):
                with timeit():
                    # Your method code here
                    result = self.do_work()
                    return result
    """
    try:
        p = profile(func_name, group_name)
        yield
    finally:
        p.end()


def report_trace_info(caller_info):
    """
    Report the trace info of the current task
    """
    if not is_flow_insight_enabled():
        return

    current_task_id = get_current_task_id()

    current_class = _get_caller_class()

    if not need_record(current_class):
        return

    if is_visual_rdb_enabled():
        ray.util.debugpy._ensure_debugger_port_open_thread_safe()

    debugger_port = ray._private.worker.global_worker.debugger_port
    debugger_host = ray._private.worker.global_worker.node_ip_address

    job_id = get_current_job_id()

    try:

        get_insight_client().emit_event(
            CallBeginEvent(
                flow_id=job_id,
                source_service=caller_info.get("caller_class", (None, None))[0],
                source_instance_id=caller_info.get("caller_class", (None, None))[1],
                source_method=caller_info.get("caller_func", "_main"),
                parent_span_id=caller_info.get("caller_task_id", ""),
                target_service=current_class[0],
                target_instance_id=current_class[1],
                target_method=_get_current_task_name(),
                span_id=current_task_id,
                timestamp=int(time.time() * 1000),
            )
        )

        if is_visual_rdb_enabled():
            get_insight_client().emit_event(
                DebuggerInfoEvent(
                    flow_id=job_id,
                    service_name=current_class[0],
                    instance_id=current_class[1],
                    method_name=_get_current_task_name(),
                    span_id=current_task_id,
                    debugger_port=debugger_port,
                    debugger_host=debugger_host,
                    debugger_enabled=is_visual_rdb_enabled(),
                    timestamp=int(time.time() * 1000),
                )
            )

    except Exception as e:
        print(f"Error reporting trace info: {e}")


def get_caller_info():
    """
    Get the caller info of the current task
    """
    if not is_flow_insight_enabled():
        return
    caller_class = _get_caller_class()
    caller_func = _get_current_task_name()
    caller_task_id = get_current_task_id()
    return {
        "caller_class": caller_class,
        "caller_func": caller_func,
        "caller_task_id": caller_task_id,
    }

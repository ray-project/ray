import logging
from concurrent import futures
import grpc
from ray import cloudpickle
import ray
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
import inspect
from ray.experimental.client import stash_api_for_tests
from ray.experimental.client.common import convert_from_arg
from ray.experimental.client.common import ClientObjectRef
from ray.experimental.client.common import ClientRemoteFunc

logger = logging.getLogger(__name__)


class RayletServicer(ray_client_pb2_grpc.RayletDriverServicer):
    def __init__(self, test_mode=False):
        self.object_refs = {}
        self.function_refs = {}
        self.actor_refs = {}
        self.registered_actor_classes = {}
        self._test_mode = test_mode

    def GetObject(self, request, context=None):
        if request.id not in self.object_refs:
            return ray_client_pb2.GetResponse(valid=False)
        objectref = self.object_refs[request.id]
        logger.info("get: %s" % objectref)
        item = ray.get(objectref)
        item_ser = cloudpickle.dumps(item)
        return ray_client_pb2.GetResponse(valid=True, data=item_ser)

    def PutObject(self, request, context=None):
        obj = cloudpickle.loads(request.data)
        objectref = ray.put(obj)
        self.object_refs[objectref.binary()] = objectref
        logger.info("put: %s" % objectref)
        return ray_client_pb2.PutResponse(id=objectref.binary())

    def WaitObject(self, request, context=None) -> ray_client_pb2.WaitResponse:
        object_refs = [cloudpickle.loads(o) for o in request.object_refs]
        num_returns = request.num_returns
        timeout = request.timeout
        object_refs_ids = []
        for object_ref in object_refs:
            if object_ref.id not in self.object_refs:
                return ray_client_pb2.WaitResponse(valid=False)
            object_refs_ids.append(self.object_refs[object_ref.id])
        try:
            ready_object_refs, remaining_object_refs = ray.wait(
                object_refs_ids,
                num_returns=num_returns,
                timeout=timeout if timeout != -1 else None)
        except Exception:
            # TODO(ameer): improve exception messages.
            return ray_client_pb2.WaitResponse(valid=False)
        logger.info("wait: %s %s" % (str(ready_object_refs),
                                     str(remaining_object_refs)))
        ready_object_ids = [
            ready_object_ref.binary() for ready_object_ref in ready_object_refs
        ]
        remaining_object_ids = [
            remaining_object_ref.binary()
            for remaining_object_ref in remaining_object_refs
        ]
        return ray_client_pb2.WaitResponse(
            valid=True,
            ready_object_ids=ready_object_ids,
            remaining_object_ids=remaining_object_ids)

    def Schedule(self, task, context=None) -> ray_client_pb2.ClientTaskTicket:
        logger.info("schedule: %s %s" %
                    (task.name,
                     ray_client_pb2.ClientTask.RemoteExecType.Name(task.type)))
        if task.type == ray_client_pb2.ClientTask.FUNCTION:
            return self._schedule_function(task, context)
        elif task.type == ray_client_pb2.ClientTask.ACTOR:
            return self._schedule_actor(task, context)
        elif task.type == ray_client_pb2.ClientTask.METHOD:
            return self._schedule_method(task, context)
        else:
            raise NotImplementedError(
                "Unimplemented Schedule task type: %s" %
                ray_client_pb2.ClientTask.RemoteExecType.Name(task.type))

    def _schedule_method(self, task: ray_client_pb2.ClientTask,
                         context=None) -> ray_client_pb2.ClientTaskTicket:
        actor_handle = self.actor_refs.get(task.payload_id)
        if actor_handle is None:
            raise Exception(
                "Can't run an actor the server doesn't have a handle for")
        arglist = _convert_args(task.args)
        with stash_api_for_tests(self._test_mode):
            output = getattr(actor_handle, task.name).remote(*arglist)
            self.object_refs[output.binary()] = output
        return ray_client_pb2.ClientTaskTicket(return_id=output.binary())

    def _schedule_actor(self, task: ray_client_pb2.ClientTask,
                        context=None) -> ray_client_pb2.ClientTaskTicket:
        with stash_api_for_tests(self._test_mode):
            if task.payload_id not in self.registered_actor_classes:
                actor_class_ref = self.object_refs[task.payload_id]
                actor_class = ray.get(actor_class_ref)
                if not inspect.isclass(actor_class):
                    raise Exception("Attempting to schedule actor that "
                                    "isn't a ClientActorClass.")
                reg_class = ray.remote(actor_class)
                self.registered_actor_classes[task.payload_id] = reg_class
            remote_class = self.registered_actor_classes[task.payload_id]
            arglist = _convert_args(task.args)
            actor = remote_class.remote(*arglist)
            actor_ref = actor._actor_id
            self.actor_refs[actor_ref.binary()] = actor
        return ray_client_pb2.ClientTaskTicket(return_id=actor_ref.binary())

    def _schedule_function(self, task: ray_client_pb2.ClientTask,
                           context=None) -> ray_client_pb2.ClientTaskTicket:
        if task.payload_id not in self.function_refs:
            funcref = self.object_refs[task.payload_id]
            func = ray.get(funcref)
            if not isinstance(func, ClientRemoteFunc):
                raise Exception("Attempting to schedule function that "
                                "isn't a ClientRemoteFunc.")
            self.function_refs[task.payload_id] = func
        remote_func = self.function_refs[task.payload_id]
        arglist = _convert_args(task.args)
        # Prepare call if we're in a test
        with stash_api_for_tests(self._test_mode):
            output = remote_func.remote(*arglist)
            self.object_refs[output.binary()] = output
        return ray_client_pb2.ClientTaskTicket(return_id=output.binary())


def _convert_args(arg_list):
    out = []
    for arg in arg_list:
        t = convert_from_arg(arg)
        if isinstance(t, ClientObjectRef):
            out.append(ray.ObjectRef(t.id))
        else:
            out.append(t)
    return out


def serve(connection_str, test_mode=False):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    task_servicer = RayletServicer(test_mode=test_mode)
    ray_client_pb2_grpc.add_RayletDriverServicer_to_server(
        task_servicer, server)
    server.add_insecure_port(connection_str)
    server.start()
    return server


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    # TODO(barakmich): Perhaps wrap ray init
    ray.init()
    server = serve("0.0.0.0:50051")
    try:
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)

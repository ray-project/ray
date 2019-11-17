import logging
import pickle

import ray
from ray import signature
from ray.function_manager import FunctionDescriptor

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@ray.remote
class ActorA:
    def submit(self):
        actor_b = ActorB._remote(is_direct_call=True)
        self.submit_to(actor_b)

    def f(self):
        return "ActorA"

    def submit_to(self, actor_b):
        if type(actor_b) is bytes:
            actor_b = pickle.loads(actor_b)
        logger.warning("actor_b %s", actor_b)
        # func = FunctionDescriptor("ray.cloudpickle.cloudpickle", "f", "ActorB")
        func = FunctionDescriptor(__name__, "f", "ActorB")

        worker = ray.worker.get_global_worker()
        worker.check_connected()

        object_ids = worker.core_worker.submit_actor_task(
            actor_b._ray_actor_id,
            func.get_function_descriptor_list(), [],
            1, {})
        ray.get(object_ids)

    def double(self, x):
        return x * 2

    def test_bytes(self, data: bytes):
        return data


@ray.remote
class ActorB:
    def f(self):
        print("call ActorB.f")
        return "ActorB"


def test_actor_call():
    actor_a = ActorA._remote(is_direct_call=True)
    worker = ray.worker.get_global_worker()
    func = FunctionDescriptor(__name__, "f", "ActorA")
    object_ids = worker.core_worker.submit_actor_task(
        actor_a._ray_actor_id,
        func.get_function_descriptor_list(), [],
        1, {})
    print("test_actor_call", ray.get(object_ids))
    ray.get(actor_a.submit.remote())


def test_actor_handle_serialization():
    import pickle
    actor_a = ActorA._remote(is_direct_call=True)
    actor_b = ActorB._remote(is_direct_call=True)
    logger.warning("actor_a %s, actor_b %s", actor_a, actor_b)
    ray.get(actor_a.submit_to.remote(pickle.dumps(actor_b)))


def test_actor_call_with_args():
    actor_a = ActorA._remote(is_direct_call=True)
    worker = ray.worker.get_global_worker()
    func = FunctionDescriptor(__name__, "double", "ActorA")
    object_ids = worker.core_worker.submit_actor_task(
        actor_a._ray_actor_id,
        func.get_function_descriptor_list(), [signature.DUMMY_TYPE, 10],
        1, {})
    print("ActorA.double", ray.get(object_ids))


def test_bytes():
    actor_a = ActorA._remote(is_direct_call=True)
    worker = ray.worker.get_global_worker()
    func = FunctionDescriptor(__name__, "test_bytes", "ActorA")
    object_ids = worker.core_worker.submit_actor_task(
        actor_a._ray_actor_id,
        func.get_function_descriptor_list(), [signature.DUMMY_TYPE, b'bytes'],
        1, {})
    print("ActorA.test_bytes", ray.get(object_ids))


if __name__ == "__main__":
    ray.init()
    # test_actor_call()
    # test_actor_handle_serialization()
    # test_actor_call_with_args()
    test_bytes()

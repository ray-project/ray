import pickle
import ray
import logging
from ray.function_manager import FunctionDescriptor
from ray.actor import ActorHandle
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


@ray.remote
class ActorB:
    def f(self):
        print("call ActorB.f")
        return "ActorB"


def test_actor_call():
    actor_a = ActorA._remote(is_direct_call=True)
    ray.get(actor_a.submit.remote())


def test_actor_handle_serialization():
    import pickle
    actor_a = ActorA._remote(is_direct_call=True)
    actor_b = ActorB._remote(is_direct_call=True)
    logger.warning("actor_a %s, actor_b %s", actor_a, actor_b)
    ray.get(actor_a.submit_to.remote(pickle.dumps(actor_b)))


if __name__ == "__main__":
    ray.init()
    # test_actor_call()
    test_actor_handle_serialization()

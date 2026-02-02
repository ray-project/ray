import ray
from ray.actor import ActorClass, ActorProxy
from ray.types import ObjectRef

ray.init()


class AsyncActor:
    @ray.method
    async def add(self, a: int, b: int) -> int:
        return a + b

    @ray.method(num_returns=1)
    async def mul(self, a: int, b: int) -> int:
        return a * b


ActorAsync: ActorClass[AsyncActor] = ray.remote(AsyncActor)
actor: ActorProxy[AsyncActor] = ActorAsync.remote()

ref_add: ObjectRef[int] = actor.add.remote(1, 2)
ref_mul: ObjectRef[int] = actor.mul.remote(2, 3)

result_add: int = ray.get(ref_add)
result_mul: int = ray.get(ref_mul)

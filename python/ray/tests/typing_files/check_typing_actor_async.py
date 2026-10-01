import ray
from ray import ObjectRef


@ray.remote
class AsyncActor:
    @ray.method
    async def add(self, a: int, b: int) -> int:
        return a + b

    @ray.method(num_returns=1)
    async def mul(self, a: int, b: int) -> int:
        return a * b

    @ray.method(num_returns=1)
    def divide(self, a: int, b: int) -> int:
        if b == 0:
            raise ValueError("Division by zero")
        return a // b

    @ray.method(num_returns=1)
    def echo(self, x: str) -> str:
        return x


actor = AsyncActor.remote()

ref_add: ObjectRef[int] = actor.add.remote(1, 2)
ref_mul: ObjectRef[int] = actor.mul.remote(2, 3)
ref_echo: ObjectRef[str] = actor.echo.remote("hello")
ref_divide: ObjectRef[int] = actor.divide.remote(10, 2)

# ray.get() should resolve to int for both
result_add: int = ray.get(ref_add)
result_mul: int = ray.get(ref_mul)
result_echo: str = ray.get(ref_echo)
result_divide: int = ray.get(ref_divide)

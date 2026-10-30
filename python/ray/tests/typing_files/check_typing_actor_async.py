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

    @ray.method
    def with_default(self, value: int = 1) -> int:
        return value

    @ray.method
    async def async_with_default(self, value: int = 1) -> int:
        return value

    @ray.method(num_returns=1)
    def with_default_config(self, value: int = 1) -> int:
        return value

    @ray.method
    def with_multiple_defaults(self, first: int = 1, second: int = 2) -> int:
        return first + second

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
ref_default: ObjectRef[int] = actor.with_default.remote()
ref_default_explicit: ObjectRef[int] = actor.with_default.remote(3)
ref_default_ref: ObjectRef[int] = actor.with_default.remote(ref_mul)
ref_async_default: ObjectRef[int] = actor.async_with_default.remote()
ref_default_config: ObjectRef[int] = actor.with_default_config.remote()
ref_default_config_explicit: ObjectRef[int] = actor.with_default_config.remote(4)
ref_multiple_defaults: ObjectRef[int] = actor.with_multiple_defaults.remote()
ref_multiple_defaults_one: ObjectRef[int] = actor.with_multiple_defaults.remote(1)
ref_multiple_defaults_two: ObjectRef[int] = actor.with_multiple_defaults.remote(1, 2)
ref_divide: ObjectRef[int] = actor.divide.remote(10, 2)

# ray.get() should resolve to int for both
result_add: int = ray.get(ref_add)
result_mul: int = ray.get(ref_mul)
result_echo: str = ray.get(ref_echo)
result_divide: int = ray.get(ref_divide)
result_default: int = ray.get(ref_default)
result_default_explicit: int = ray.get(ref_default_explicit)
result_default_ref: int = ray.get(ref_default_ref)
result_default_config: int = ray.get(ref_default_config)
result_default_config_explicit: int = ray.get(ref_default_config_explicit)
result_multiple_defaults: int = ray.get(ref_multiple_defaults)
result_multiple_defaults_one: int = ray.get(ref_multiple_defaults_one)
result_multiple_defaults_two: int = ray.get(ref_multiple_defaults_two)

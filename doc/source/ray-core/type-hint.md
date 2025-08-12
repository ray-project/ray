# Type hints in Ray

Ray provides comprehensive support for Python type hints with both remote functions and actors. This enables better IDE support, static type checking, and improved code maintainability in distributed Ray applications.

## Overview

In most cases, Ray applications can use type hints without any modifications to existing code. Ray automatically handles type inference for standard remote functions and basic actor usage patterns.

However, certain patterns require specific approaches to ensure proper type annotation, particularly when working with actor references.

## Build an actor

In Ray, the `@ray.remote` decorator indicates that instances of the class `T` are actors, with each actor running in its own Python process. However, the `@ray.remote` decorator will transform the class `T` into a `ActorClass[T]` type, which is not the original class type. Unfortunately, IDE and static type checkers will not be able to infer the original type `T` of the `ActorClass[T]`. To solve this problem, using `ray.remote(T)` will explicitly return a new generic class `ActorClass[T]` type while preserving the original class type.

> Note: Recommend to use `ray.remote(T)` to build an `ActorClass[T]` from class `T` instead of using `@ray.remote` decorator.

```python
import ray
from ray.actor import ActorClass

class DemoRay:
    def __init__(self, init: int):
        self.init = init

    @ray.method
    def calculate(self, v1: int, v2: int) -> int:
        return self.init + v1 + v2

ActorDemoRay: ActorClass[DemoRay] = ray.remote(DemoRay)
# DemoRay is the original class type, ActorDemoRay is the ActorClass[DemoRay] type


# ActorProxy[DemoRay] can be used as the type hint for an actor.
@ray.remote
def func(actor: ActorProxy[DemoRay]) -> int:
    b: ObjectRef[int] = actor.calculate.remote(1, 2)
    return ray.get(b)

a = func.remote(ActorDemoRay.remote(1))
print(ray.get(a))
```

## Get actor remote methods

Regardless of how an `ActorProxy[T]` is constructed, by adding `@ray.method` decorator to the actor methods, Ray will be able to correctly provide type hints for the remote methods of the actor through `ActorProxy[T]` type, including their arguments and return types.

> Note: `@ray.method` decorator is essential for actor methods as it enables Ray to properly infer method types and maintain type safety across remote calls.

```python
from ray.actor import ActorClass, ActorProxy

class DemoRay:
    def __init__(self, init: int):
        self.init = init

    @ray.method
    def calculate(self, v1: int, v2: int) -> int:
        return self.init + v1 + v2

ActorDemoRay: ActorClass[DemoRay] = ray.remote(DemoRay)
actor: ActorProxy[DemoRay] = ActorDemoRay.remote(1)
# IDEs will be able to correctly list the remote methods of the actor
# and provide type hints for the arguments and return values of the remote methods
a = actor.calculate.remote(1, 2)
print(ray.get(a))
```

> Note: We would love to make the typing of remote methods work without `@ray.method` decorator. If any community member has an idea, we welcome PRs.


## Remote functions

Ray remote functions support standard Python type annotations without additional configuration. The `@ray.remote` decorator preserves the original function signature and type information.

```python
import ray

@ray.remote
def add_numbers(x: int, y: int) -> int:
    return x + y

# Type hints work seamlessly with remote function calls
a = add_numbers.remote(5, 3)
print(ray.get(a))
```

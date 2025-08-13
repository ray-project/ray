# Type hints in Ray

As of Ray 2.48, Ray provides comprehensive support for Python type hints with both remote functions and actors. This enables better IDE support, static type checking, and improved code maintainability in distributed Ray applications.

## Overview

In most cases, Ray applications can use type hints without any modifications to existing code. Ray automatically handles type inference for standard remote functions and basic actor usage patterns. For example, remote functions support standard Python type annotations without additional configuration. The `@ray.remote` decorator preserves the original function signature and type information.

```python
import ray

@ray.remote
def add_numbers(x: int, y: int) -> int:
    return x + y

# Type hints work seamlessly with remote function calls
a = add_numbers.remote(5, 3)
print(ray.get(a))
```

However, certain patterns, especially when working with actors, require specific approaches to ensure proper type annotation.

## Pattern 1: Use `ray.remote` as a function to build an actor

Use the `ray.remote` function directly to create an actor class, instead of using the `@ray.remote` decorator. This will preserve the original class type and allow type inference to work correctly. For example, in this case, the original class type is `DemoRay`, and the actor class type is `ActorClass[DemoRay]`.

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
```

After creating the `ActorClass[DemoRay]` type, we can use it to instantiate an actor by calling `ActorDemoRay.remote(1)`. It returns an `ActorProxy[DemoRay]` type, which represents an actor handle.

This handle will provide type hints for the actor methods, including their arguments and return types.

```python

actor: ActorProxy[DemoRay] = ActorDemoRay.remote(1)

def func(actor: ActorProxy[DemoRay]) -> int:
    b: ObjectRef[int] = actor.calculate.remote(1, 2)
    return ray.get(b)

a = func.remote()
print(ray.get(a))
```

**Why do we need to do this?**

In Ray, the `@ray.remote` decorator indicates that instances of the class `T` are actors, with each actor running in its own Python process. However, the `@ray.remote` decorator will transform the class `T` into a `ActorClass[T]` type, which is not the original class type.

Unfortunately, IDE and static type checkers will not be able to infer the original type `T` of the `ActorClass[T]`. To solve this problem, using `ray.remote(T)` will explicitly return a new generic class `ActorClass[T]` type while preserving the original class type.

## Pattern 2: Use `@ray.method` decorator for remote methods

Add the `@ray.method` decorator to the actor methods in order to obtain type hints for the remote methods of the actor through `ActorProxy[T]` type, including their arguments and return types.

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
a: ObjectRef[int] = actor.calculate.remote(1, 2)
print(ray.get(a))
```

!!! note
    We would love to make the typing of remote methods work without `@ray.method` decorator. If any community member has an idea, we welcome PRs.

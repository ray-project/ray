---
myst:
  html_meta:
    description: "Use Python type hints with Ray remote functions and actors for IDE support and static type checking through ray.remote and @ray.method."
---

(core-type-hint)=

# Type hints in Ray

As of Ray 2.48, Ray supports Python type hints for both remote functions and actors. Type hints give you better IDE support, static type checking, and more maintainable code in distributed Ray applications.

## Overview

In most cases, type hints work in Ray applications without changes to existing code. Ray handles type inference automatically for standard remote functions and basic actor usage patterns. For example, remote functions support standard Python type annotations without extra configuration. The `@ray.remote` decorator preserves the original function signature and type information.

```python
import ray

@ray.remote
def add_numbers(x: int, y: int) -> int:
    return x + y

# Type hints work seamlessly with remote function calls
a = add_numbers.remote(5, 3)
print(ray.get(a))
```

Some patterns, especially with actors, need a specific approach for type annotations to work correctly.

## Pattern 1: Use `ray.remote` as a function to build an actor

To create an actor class, call `ray.remote` as a function instead of using the `@ray.remote` decorator. Calling it as a function preserves the original class type, so type inference works correctly. In the following example, the original class type is `DemoRay`, and the actor class type is `ActorClass[DemoRay]`.

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

To instantiate an actor from the `ActorClass[DemoRay]` type, call `ActorDemoRay.remote(1)`. The call returns an `ActorProxy[DemoRay]` type, which represents an actor handle.

The handle provides type hints for the actor methods, including their arguments and return types.

```python

actor: ActorProxy[DemoRay] = ActorDemoRay.remote(1)

def func(actor: ActorProxy[DemoRay]) -> int:
    b: ObjectRef[int] = actor.calculate.remote(1, 2)
    return ray.get(b)

a = func.remote()
print(ray.get(a))
```

### Why is this pattern necessary?

In Ray, the `@ray.remote` decorator indicates that instances of class `T` are actors, each running in its own Python process. The decorator also transforms class `T` into an `ActorClass[T]` type, which isn't the original class type.

IDEs and static type checkers can't infer the original type `T` from `ActorClass[T]`. Calling `ray.remote(T)` solves this problem because it explicitly returns a generic `ActorClass[T]` type while preserving the original class type.

## Pattern 2: Use `@ray.method` decorator for remote methods

Add the `@ray.method` decorator to actor methods to get type hints for them through the `ActorProxy[T]` type, including their arguments and return types.

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

:::{note}
The Ray project would like typing for remote methods to work without the `@ray.method` decorator. If you have an idea for how, open a PR.
:::

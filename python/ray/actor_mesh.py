import copy
import math
import os
import random
from typing import Any, Callable, Concatenate, Dict, Generic, List, Optional, Tuple, TypeVar, ParamSpec, Union
from typing import overload

import ray
from ray.actor import ActorHandle, ActorProxy

T = TypeVar("T", covariant=True)

P = ParamSpec("P")
R = TypeVar("R")

class ActorMeshProxy:
    def __init__(self, actor_mesh: "ActorMesh") -> None:
        self.actor_mesh = actor_mesh

    def __getattr__(self, k):
        return ActorMethodProxy(self.actor_mesh, k)
    
class ActorMethod(Generic[P, R]):

    def __init__(self, method):
        self.method = method

    def all(self, *args: P.args, **kwargs: P.kwargs) -> List["ray.ObjectRef[R]"]:
        "Call all the actors in the mesh with the same arguments."
        pass

    def choose(self, *args: P.args, **kwargs: P.kwargs) -> "ray.ObjectRef[R]":
        "Call one of the actors in the mesh."
        pass
        
    def shard(self, *args: P.args, **kwargs: P.kwargs) -> R:
        "Call all the actors in the mesh, shard the argument(s) among them."
        pass
    
class ActorMethodProxy:

    def __init__(self, actor_mesh: "ActorMesh", actor_method: str) -> None:
        self.actor_mesh = actor_mesh
        self.actor_method = actor_method

    def all(self, *args, **kwargs):
        results = []
        for actor in self.actor_mesh.actors:
            method = getattr(actor, self.actor_method)
            results.append(method.remote(*args, **kwargs))
        return results
    
    def choose(self, *args, **kwargs):
        # Choosing randomly right now, this is bad and needs to be improved
        actor = random.choice(self.actor_mesh.actors)
        method = getattr(actor, self.actor_method)
        return method.remote(*args, **kwargs)
    
    def shard(self, *args, **kwargs):
        orig_func = getattr(self.actor_mesh._actor_cls, self.actor_method)
        dispatch_fn = getattr(orig_func, "_dispatch_fn", None)
        assert dispatch_fn, "Sharding only supported if dispatch_fn is specified (later we will have a good default one)"
        return dispatch_fn(self.actor_mesh, self.actor_method, *args, **kwargs)


class ActorMesh(Generic[T]):

    def __init__(
            self,
            actor_cls: Callable[..., T],
            args: Any,
            kwargs: Any,
            shape: Union[int, Tuple[int], Dict[str, int]],
            # TODO: Not used yet
            resources_per_actor: Optional[Dict[str, float]] = None,
            runtime_env: Optional[Dict[str, Any]] = None,
        ):

        if isinstance(shape, int):
            shape = (shape,)

        self.shape = shape

        if isinstance(shape, dict):
            self._num_actors = math.prod(shape.values())
        else:
            self._num_actors = math.prod(shape)

        self._actor_cls = actor_cls
        self._actors = []
        runtime_env = copy.deepcopy(runtime_env) or {}
        for i in range(self._num_actors):
            env_vars = {**runtime_env.get("env_vars", {}), "RAY_ACTOR_MESH_RANK": str(i)}
            ray_actor_cls = ray.remote(
                runtime_env={**runtime_env, "env_vars": env_vars},
            )(actor_cls)
            actor = ray_actor_cls.remote(*args, **kwargs)
            self._actors.append(actor)
    
    @property
    def methods(self) -> type[T]:
        return ActorMeshProxy(self)

    @property
    def actors(self) -> List[ActorHandle]:
        return self._actors
    
    @property
    def num_actors(self) -> int:
        return self._num_actors

@overload
def method(method: Callable[Concatenate[Any, P], R]) -> ActorMethod[P, R]:
    ...

@overload
def method(dispatch_fn: Optional[Callable] = None,) -> Callable[[Callable[Concatenate[Any, P], R]], ActorMethod[P, R]]:
    ...

def method(method=None, dispatch_fn=None, **kwargs):

    def decorator(f):
        method = ray.method(**kwargs)(f)
        method._dispatch_fn = dispatch_fn
        return method

    if method is not None:
        return decorator(method)
    return decorator

if __name__ == "__main__":

    import torch

    # The following is an example for the usage of the actor pool:

    def python_dispatch_fn(actor_mesh: ActorMesh, actor_method: str, *args, **kwargs):
        # This method only shards the first argument, but similarly other
        # arguments could be sharded as well.
        import numpy as np
        sharded_args0 = np.array_split(args[0], actor_mesh.num_actors)
        results = []
        for i, actor in enumerate(actor_mesh.actors):
            method = getattr(actor, actor_method)
            results.append(method.remote(*(sharded_args0[i], *args[1:]), **kwargs))
        return sum(ray.get(results), [])

    def torch_dispatch_fn(actor_mesh: ActorMesh, actor_method: str, *args, **kwargs):
        sharded_args0 = torch.tensor_split(args[0], actor_mesh.num_actors)
        results = []
        for i, actor in enumerate(actor_mesh.actors):
            method = getattr(actor, actor_method)
            results.append(method.remote(*(sharded_args0[i], *args[1:]), **kwargs))
        return torch.concat(ray.get(results))

    class Test:

        def __init__(self):
            print("actor rank = ", os.environ["RAY_ACTOR_MESH_RANK"])

        @method
        def f(self, x: int) -> int:
            return x
        
        @method
        def add(self, x: int, y: int) -> int:
            return x + y
        
        @method(dispatch_fn=python_dispatch_fn)
        def process(self, values: List[int], y: int) -> List[int]:
            return [x + y for x in values]

        @method(dispatch_fn=torch_dispatch_fn)
        def torch_process(self, batch: torch.Tensor, val: int) -> torch.Tensor:
            return batch + val


    mesh = ActorMesh(Test, (), {}, shape=(3,))
    # Note: In the future we could have the alternative syntax
    # similar to what Stephanie suggested:
    # mesh = Test.mesh(shape=(3,)).remote()
    result = mesh.methods.f.all(1)
    assert ray.get(result) == [1, 1, 1]
    result = mesh.methods.f.choose(1)
    assert ray.get(result) == 1
    result = mesh.methods.process.shard([1, 2, 3, 4, 5, 6], 5)
    assert result == [6, 7, 8, 9, 10, 11]
    batch = torch.eye(6)
    result = mesh.methods.torch_process.shard(batch, 1)
    assert (result == torch.eye(6) + 1).all()

    # The case of a more complex shape
    mesh = ActorMesh(Test, (), {}, shape=(2, 3))
    assert mesh.num_actors == 6

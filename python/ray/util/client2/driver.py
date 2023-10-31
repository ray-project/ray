import ray
import time
import asyncio
import argparse
import ray._private.ray_constants as ray_constants
from typing import Dict
from ray.util.client2.pickler import (
    ServerToClientPickler,
    ClientToServerUnpickler,
    dumps_with_pickler_cls,
    loads_with_unpickler_cls,
)

"""
A script that spins up a detached long running actor.

The actor takes get/put/remote() calls and runs within its own job. It keeps references
to all objects and actors to avoid them from being gc'd.

# TODO: how to GC objects/actors?
"""


ray.init()


@ray.remote
class ClientSupervisor:
    def __init__(self, name: str) -> None:
        # if client died and restarted, it can reconnect via this name as actor name
        self.actor_name = name
        # In order to use ray.util.client.server.server_pickler, we have to have these member names:
        # TODO: what about streamed generators?
        # TODO: remote func and actor and method
        self.object_refs: Dict[bytes, ray.ObjectRef] = {}
        self.actor_refs: Dict[bytes, ray.actor.ActorHandle] = {}

        self.dumps = dumps_with_pickler_cls(ServerToClientPickler, server=self)
        self.loads = loads_with_unpickler_cls(ClientToServerUnpickler, server=self)

        print(f"ClientSupervisor inited! {self.actor_name}")

    async def get_name(self):
        print(f"get_name: {self.actor_name}, encoded {self.actor_name.encode()}")
        return self.actor_name.encode()

    async def ray_get(self, obj_ref_serialized):
        print(f"ray_get {obj_ref_serialized}")
        obj_refs = self.loads(obj_ref_serialized)
        # we can get ray.ObjectRef or List[ray.ObjectRef]
        if isinstance(obj_refs, ray.ObjectRef):
            obj_ref = obj_refs
            if obj_ref.binary() not in self.object_refs:
                raise ValueError(
                    f"Unknown object ref {obj_ref}. Maybe it's in another "
                    " client2 session, or the old session had died?"
                )
            obj = await obj_ref
        else:
            unknowns = [
                obj_ref
                for obj_ref in obj_refs
                if obj_ref.binary() not in self.object_refs
            ]
            if len(unknowns) > 0:
                raise ValueError(
                    f"Unknown object refs {unknowns}. Maybe they're in another "
                    " client2 session, or the old session had died?"
                )
            objs = await asyncio.gather(*obj_refs)
            return self.dumps(objs)
        return self.dumps(obj)

    async def ray_put(self, obj_serialized):
        print(f"ray_put {obj_serialized}")
        obj = self.loads(obj_serialized)
        obj_ref = ray.put(obj)
        self.object_refs[obj_ref.binary()] = obj_ref  # keeping the ref...
        print(f"put {obj} as {obj_ref}, all {self.object_refs}")
        return self.dumps(obj_ref)

    async def task_remote(self, pickled_func_and_args):
        print(f"task_remote {pickled_func_and_args}")
        func, args, kwargs, task_options = self.loads(pickled_func_and_args)
        obj_ref = ray.remote(func).options(**task_options).remote(*args, **kwargs)
        self.object_refs[obj_ref.binary()] = obj_ref  # keeping the obj refs...
        return self.dumps(obj_ref)

    async def actor_remote(self, pickled_cls_and_args):
        """
        Returns (actor_id, actor_method_cpu, current_session_and_job)
        """
        actor_cls, args, kwargs, task_options = self.loads(pickled_cls_and_args)
        actor_handle = actor_cls.options(**task_options).remote(*args, **kwargs)
        self.actor_refs[actor_handle._actor_id.binary()] = actor_handle
        print("actor_remote args ", actor_cls, args, kwargs)
        return self.dumps(
            (
                actor_handle._actor_id,
                actor_handle._ray_actor_method_cpus,
                actor_handle._ray_session_and_job,
            )
        )

    async def method_remote(self, pickled_cls_and_args):
        """
        Returns ObjectRef.
        """
        actor_id, method_name, args, kwargs, task_options = self.loads(
            pickled_cls_and_args
        )
        if actor_id.binary() not in self.actor_refs:
            raise ValueError(
                f"Unknown actor handle {actor_id}. Maybe it's in another "
                " client2 session, or the old session had died?"
            )
        actor = self.actor_refs[actor_id.binary()]
        print(
            "method_remote args ",
            actor,
            actor_id,
            method_name,
            args,
            kwargs,
            task_options,
        )
        print(f"self {self.actor_refs}, {self.object_refs}")
        obj_ref = (
            getattr(actor, method_name).options(**task_options).remote(*args, **kwargs)
        )
        return self.dumps(obj_ref)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Client2 driver.")
    parser.add_argument("actor_name", type=str, help="The name of the actor")

    args = parser.parse_args()

    # Detached: makes sure the actor is visible by other jobs.
    # get_if_exists=True: to avoid race conditions on starting actors.
    actor = ClientSupervisor.options(
        name=args.actor_name,
        namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE,
        lifetime="detached",
        get_if_exists=True,
    ).remote(args.actor_name)
    print(
        f"created driver name {actor.get_name.remote()}, "
        f"ns {ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE}: {actor}"
    )

    while True:
        time.sleep(1000)

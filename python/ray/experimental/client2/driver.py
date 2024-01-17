import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime
from typing import Coroutine, Dict

import ray
import ray._private.ray_constants as ray_constants
from ray.experimental.client2.datatypes import ResultOrException
from ray.experimental.client2.pickler import (
    ClientToServerUnpickler,
    ServerToClientPickler,
    dumps_with_pickler_cls,
    loads_with_unpickler_cls,
)

"""
A script that spins up a detached long running actor.

The actor takes get/put/remote() calls and runs within its own job. It keeps references
to all objects and actors to avoid them from being gc'd.

TODO: how to GC objects/actors?
"""


ray.init()


def format_time(timestamp: int):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


@ray.remote
class ClientSupervisor:
    def __init__(self, name: str, ttl_secs: int) -> None:
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.logger.debug(
            f"ClientSupervisor init, name = {name}, ttl_secs = {ttl_secs}"
        )

        # if client died and restarted, it can reconnect via this name as actor name
        self.actor_name = name
        self.ttl_secs = ttl_secs

        self.reset_ttl_timer()
        self.watchdog_task = asyncio.create_task(self.ttl_watchdog())
        self.watchdog_task.add_done_callback(
            lambda task: self.logger.info(f"watchdog is done! {task.result()}")
        )

        # In order to use ray.util.client.server.server_pickler, we have to have these member names:
        # TODO: what about streamed generators?
        self.object_refs: Dict[bytes, ray.ObjectRef] = {}
        self.actor_refs: Dict[bytes, ray.actor.ActorHandle] = {}

        self.dumps = dumps_with_pickler_cls(ServerToClientPickler, server=self)
        self.loads = loads_with_unpickler_cls(ClientToServerUnpickler, server=self)

        self.logger.debug(f"ClientSupervisor inited! {self.actor_name}")

    def reset_ttl_timer(self):
        self.last_activity_time = time.time()

    async def serialize_result_or_exc(self, f: Coroutine) -> bytes:
        """
        Executes f, captures any result or exceptions into a ResultOrException and serializes it into bytes.
        """
        result_or_exc = ResultOrException()
        try:
            result = await f
            result_or_exc.result = result
        except BaseException as e:
            result_or_exc.exc = e
        return self.dumps(result_or_exc)

    async def ttl_watchdog(self):
        while True:
            current_time = time.time()
            target_time = self.last_activity_time + self.ttl_secs
            should_kill_actor = target_time < current_time
            self.logger.debug(
                f"ttl_watchdog: current time = {format_time(current_time)}, "
                f"last activity = {format_time(self.last_activity_time)}, "
                f"ttl = {self.ttl_secs}s, "
                f"should kill actor = {should_kill_actor}"
            )
            if should_kill_actor:
                # TODO: For the asyncio vs exception reason, the recommended
                # ray.actor.exit_actor()
                # can not be used - the exception it raised is ignored.
                sys.exit(0)
            else:
                # +1s to have a better chance of seeing the overtime
                await asyncio.sleep(target_time - current_time + 1)

    async def get_name(self):
        self.reset_ttl_timer()
        return self.actor_name

    async def get_job_id(self) -> str:
        self.reset_ttl_timer()
        runtime_context = ray.get_runtime_context()
        return runtime_context.get_job_id()

    async def ray_get(self, obj_ref_serialized):
        return await self.serialize_result_or_exc(
            self.ray_get_internal(obj_ref_serialized)
        )

    async def ray_get_internal(self, obj_ref_serialized):
        self.reset_ttl_timer()
        self.logger.debug(f"ray_get {obj_ref_serialized}")
        obj_refs = self.loads(obj_ref_serialized)
        # we can get ray.ObjectRef or List[ray.ObjectRef]
        if isinstance(obj_refs, ray.ObjectRef):
            obj_ref = obj_refs
            if obj_ref.binary() not in self.object_refs:
                raise ValueError(
                    f"Unknown object ref {obj_ref}. Maybe it's in another "
                    "client2 session, or the old session had died?"
                )
            return await obj_ref
        # List[ray.ObjectRef]
        unknowns = [
            obj_ref for obj_ref in obj_refs if obj_ref.binary() not in self.object_refs
        ]
        if len(unknowns) > 0:
            raise ValueError(
                f"Unknown object refs {unknowns}. Maybe they're in another "
                "client2 session, or the old session had died?"
            )
        return await asyncio.gather(*obj_refs)

    async def ray_put(self, obj_serialized):
        return await self.serialize_result_or_exc(self.ray_put_internal(obj_serialized))

    async def ray_put_internal(self, obj_serialized):
        self.reset_ttl_timer()
        self.logger.debug(f"ray_put {obj_serialized}")
        obj = self.loads(obj_serialized)
        obj_ref = ray.put(obj)
        self.object_refs[obj_ref.binary()] = obj_ref  # keeping the ref...
        return obj_ref

    async def task_remote(self, pickled_func_and_args):
        return await self.serialize_result_or_exc(
            self.task_remote_internal(pickled_func_and_args)
        )

    async def task_remote_internal(self, pickled_func_and_args):
        self.reset_ttl_timer()
        func, args, kwargs, task_options = self.loads(pickled_func_and_args)
        if task_options is not None:
            func = func.options(**task_options)
        obj_ref = func.remote(*args, **kwargs)
        self.object_refs[obj_ref.binary()] = obj_ref  # keeping the obj refs...
        return obj_ref

    async def actor_remote(self, pickled_cls_and_args):
        return await self.serialize_result_or_exc(
            self.actor_remote_internal(pickled_cls_and_args)
        )

    async def actor_remote_internal(self, pickled_cls_and_args):
        """
        Returns (actor_id, actor_method_cpu, current_session_and_job)
        """
        self.reset_ttl_timer()
        actor_cls, args, kwargs, task_options = self.loads(pickled_cls_and_args)
        if task_options is not None:
            actor_cls = actor_cls.options(**task_options)
        actor_handle = actor_cls.remote(*args, **kwargs)
        self.actor_refs[actor_handle._actor_id.binary()] = actor_handle
        return actor_handle

    async def method_remote(self, pickled_cls_and_args):
        return await self.serialize_result_or_exc(
            self.method_remote_intenral(pickled_cls_and_args)
        )

    async def method_remote_intenral(self, pickled_cls_and_args):
        """
        Returns ObjectRef.
        """
        self.reset_ttl_timer()
        actor_id, method_name, args, kwargs, task_options = self.loads(
            pickled_cls_and_args
        )
        if actor_id.binary() not in self.actor_refs:
            raise ValueError(
                f"Unknown actor handle {actor_id}. Maybe it's in another "
                "client2 session, or the old session had died?"
            )
        actor = self.actor_refs[actor_id.binary()]
        method = getattr(actor, method_name)
        if task_options is not None:
            method = method.options(**task_options)
        return method.remote(*args, **kwargs)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Client2 driver.")
    parser.add_argument("actor_name", type=str, help="The name of the actor")
    parser.add_argument(
        "ttl_secs",
        type=int,
        help="Seconds of time-to-live since last "
        "client request, after which this actor suicides.",
    )

    args = parser.parse_args()

    # Detached: makes sure the actor is visible by other jobs.
    # get_if_exists=True: to avoid race conditions on starting actors.
    actor = ClientSupervisor.options(
        name=args.actor_name,
        namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE,
        lifetime="detached",
        get_if_exists=True,
    ).remote(args.actor_name, args.ttl_secs)
    print(
        f"created driver name {actor.get_name.remote()}, "
        f"ns {ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE}: {actor}"
    )

    # TODO: wait for the actor to exit
    while True:
        time.sleep(1000)

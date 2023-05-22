import logging
from typing import Any, Dict, List, Optional

import ray
import ray._private.ray_constants as ray_constants
import ray._private.signature as signature
import ray._raylet
from ray._raylet import PythonFunctionDescriptor

logger = logging.getLogger(__name__)


class BatchActorsHandle:
    def __init__(self, actors: List[ray.actor.ActorHandle]) -> None:
        self.actors = actors
        actor_template = actors[0]
        self._ray_is_cross_language = actor_template._ray_is_cross_language
        self._ray_actor_language = actor_template._ray_actor_language
        self._ray_original_handle = actor_template._ray_original_handle
        self._ray_method_decorators = actor_template._ray_method_decorators
        self._ray_method_signatures = actor_template._ray_method_signatures
        self._ray_method_num_returns = actor_template._ray_method_num_returns
        self._ray_actor_method_cpus = actor_template._ray_actor_method_cpus
        self._ray_session_and_job = actor_template._ray_session_and_job
        self._ray_actor_creation_function_descriptor = (
            actor_template._ray_actor_creation_function_descriptor
        )
        self._ray_function_descriptor = actor_template._ray_function_descriptor

        if not self._ray_is_cross_language:
            assert isinstance(
                self._ray_actor_creation_function_descriptor, PythonFunctionDescriptor
            )
            module_name = self._ray_actor_creation_function_descriptor.module_name
            class_name = self._ray_actor_creation_function_descriptor.class_name
            for method_name in self._ray_method_signatures.keys():
                function_descriptor = PythonFunctionDescriptor(
                    module_name, method_name, class_name
                )
                self._ray_function_descriptor[method_name] = function_descriptor
                method = ray.actor.ActorMethod(
                    self,
                    method_name,
                    self._ray_method_num_returns[method_name],
                    decorator=self._ray_method_decorators.get(method_name),
                    hardref=self,
                )
                setattr(self, method_name, method)

    def _actor_method_call(
        self,
        method_name: str,
        args: List[Any] = None,
        kwargs: Dict[str, Any] = None,
        name: str = "",
        num_returns: Optional[int] = None,
        concurrency_group_name: Optional[str] = None,
    ):
        worker = ray._private.worker.global_worker

        args = args or []
        kwargs = kwargs or {}
        if self._ray_is_cross_language:
            list_args = ray.cross_language._format_args(worker, args, kwargs)
            function_descriptor = ray.cross_language._get_function_descriptor_for_actor_method(  # noqa: E501
                self._ray_actor_language,
                self._ray_actor_creation_function_descriptor,
                method_name,
                # The signature for xlang should be "{length_of_arguments}" to handle
                # overloaded methods.
                signature=str(len(args) + len(kwargs)),
            )
        else:
            function_signature = self._ray_method_signatures[method_name]

            if not args and not kwargs and not function_signature:
                list_args = []
            else:
                list_args = signature.flatten_args(function_signature, args, kwargs)
            function_descriptor = self._ray_function_descriptor[method_name]

        if worker.mode == ray.LOCAL_MODE:
            assert (
                not self._ray_is_cross_language
            ), "Cross language remote actor method cannot be executed locally."

        if num_returns == "dynamic":
            num_returns = -1

        object_refs = worker.core_worker.batch_submit_actor_task(
            self.actors[0]._ray_actor_language,
            [actor._actor_id for actor in self.actors],
            function_descriptor,
            list_args,
            name,
            num_returns,
            self._ray_actor_method_cpus,
            concurrency_group_name if concurrency_group_name is not None else b"",
        )

        if len(object_refs) == 0:
            object_refs = None

        return object_refs

    def __getattr__(self, item):
        if not self._ray_is_cross_language:
            raise AttributeError(
                f"'{type(self).__name__}' object has " f"no attribute '{item}'"
            )
        if item in ["__ray_terminate__"]:

            class FakeActorMethod(object):
                def __call__(self, *args, **kwargs):
                    raise TypeError(
                        "Actor methods cannot be called directly. Instead "
                        "of running 'object.{}()', try 'object.{}.remote()'.".format(
                            item, item
                        )
                    )

                def remote(self, *args, **kwargs):
                    logger.warning(
                        f"Actor method {item} is not supported by cross language."
                    )

            return FakeActorMethod()

        return ray.actor.ActorMethod(
            self,
            item,
            ray_constants.
            # Currently, we use default num returns
            DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS,
            # Currently, cross-lang actor method not support decorator
            decorator=None,
        )


def batch_remote(actors: List[ray.actor.ActorHandle]) -> BatchActorsHandle:
    return BatchActorsHandle(actors)

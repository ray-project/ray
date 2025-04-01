from typing import Any, Callable, Dict, List, Optional

from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import OldAPIStack


def make_callback(
    callback_name: str,
    callbacks_objects: Optional[List[RLlibCallback]] = None,
    callbacks_functions: Optional[List[Callable]] = None,
    *,
    args: List[Any] = None,
    kwargs: Dict[str, Any] = None,
) -> None:
    """Calls an RLlibCallback method or a registered callback callable.

    Args:
        callback_name: The name of the callback method or key, for example:
            "on_episode_start" or "on_train_result".
        callbacks_objects: The RLlibCallback object or list of RLlibCallback objects
            to call the `callback_name` method on (in the order they appear in the
            list).
        callbacks_functions: The callable or list of callables to call
            (in the order they appear in the list).
        args: Call args to pass to the method/callable calls.
        kwargs: Call kwargs to pass to the method/callable calls.
    """
    # Loop through all available RLlibCallback objects.
    callbacks_objects = force_list(callbacks_objects)
    for callback_obj in callbacks_objects:
        getattr(callback_obj, callback_name)(*(args or ()), **(kwargs or {}))

    # Loop through all available RLlibCallback objects.
    callbacks_functions = force_list(callbacks_functions)
    for callback_fn in callbacks_functions:
        callback_fn(*(args or ()), **(kwargs or {}))


@OldAPIStack
def _make_multi_callbacks(callback_class_list):
    class _MultiCallbacks(RLlibCallback):
        IS_CALLBACK_CONTAINER = True

        def __init__(self):
            super().__init__()
            self._callback_list = [
                callback_class() for callback_class in callback_class_list
            ]

        def on_algorithm_init(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_algorithm_init(**kwargs)

        def on_workers_recreated(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_workers_recreated(**kwargs)

        # Only on new API stack.
        def on_env_runners_recreated(self, **kwargs) -> None:
            pass

        def on_checkpoint_loaded(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_checkpoint_loaded(**kwargs)

        def on_create_policy(self, *, policy_id, policy) -> None:
            for callback in self._callback_list:
                callback.on_create_policy(policy_id=policy_id, policy=policy)

        def on_environment_created(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_environment_created(**kwargs)

        def on_sub_environment_created(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_sub_environment_created(**kwargs)

        def on_episode_created(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_episode_created(**kwargs)

        def on_episode_start(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_episode_start(**kwargs)

        def on_episode_step(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_episode_step(**kwargs)

        def on_episode_end(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_episode_end(**kwargs)

        def on_evaluate_start(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_evaluate_start(**kwargs)

        def on_evaluate_end(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_evaluate_end(**kwargs)

        def on_postprocess_trajectory(
            self,
            *,
            worker,
            episode,
            agent_id,
            policy_id,
            policies,
            postprocessed_batch,
            original_batches,
            **kwargs,
        ) -> None:
            for callback in self._callback_list:
                callback.on_postprocess_trajectory(
                    worker=worker,
                    episode=episode,
                    agent_id=agent_id,
                    policy_id=policy_id,
                    policies=policies,
                    postprocessed_batch=postprocessed_batch,
                    original_batches=original_batches,
                    **kwargs,
                )

        def on_sample_end(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_sample_end(**kwargs)

        def on_learn_on_batch(
            self, *, policy, train_batch, result: dict, **kwargs
        ) -> None:
            for callback in self._callback_list:
                callback.on_learn_on_batch(
                    policy=policy, train_batch=train_batch, result=result, **kwargs
                )

        def on_train_result(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_train_result(**kwargs)

    return _MultiCallbacks

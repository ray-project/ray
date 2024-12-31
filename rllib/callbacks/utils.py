from ray.rllib.callbacks.callbacks import Callbacks
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import OldAPIStack


def make_callback(
    callback_name,
    callbacks_objects=None,
    callbacks_functions=None,
    *,
    args=None,
    kwargs=None,
) -> None:
    # Loop through all available Callbacks objects.
    callbacks_objects = force_list(callbacks_objects)
    for callback_obj in callbacks_objects:
        getattr(callback_obj, callback_name)(*(args or ()), **(kwargs or {}))

    # Loop through all available Callbacks objects.
    callbacks_functions = force_list(callbacks_functions)
    for callback_fn in callbacks_functions:
        callback_fn(*(args or ()), **(kwargs or {}))


@OldAPIStack
def _make_multi_callbacks(callback_class_list):
    class _MultiCallbacks(Callbacks):
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

        def on_checkpoint_loaded(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_checkpoint_loaded(**kwargs)

        def on_create_policy(self, *, policy_id, policy) -> None:
            for callback in self._callback_list:
                callback.on_create_policy(policy_id=policy_id, policy=policy)

        def on_environment_created(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_environment_created(**kwargs)

        @OldAPIStack
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

        @OldAPIStack
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

        @OldAPIStack
        def on_learn_on_batch(
                self, *, policy, train_batch, result: dict,
                **kwargs
        ) -> None:
            for callback in self._callback_list:
                callback.on_learn_on_batch(
                    policy=policy, train_batch=train_batch, result=result, **kwargs
                )

        def on_train_result(self, **kwargs) -> None:
            for callback in self._callback_list:
                callback.on_train_result(**kwargs)

    return _MultiCallbacks



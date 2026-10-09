from typing import List

import ray
from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.algorithms.appo import APPO
from ray.rllib.env.env_runner_group import EnvRunnerGroup


@ray.remote
class SharedDataActor:
    """Simple example of an actor that's accessible from all other actors of an algo.

    Exposes remote APIs `put` and `get` to other actors for storing and retrieving
    arbitrary data.
    """

    def __init__(self):
        self.storage = {}

    def get(self, key, delete: bool = False):
        value = self.storage.get(key)
        if delete and key in self.storage:
            del self.storage[key]
        return value

    def put(self, key, value):
        self.storage[key] = value

    def get_state(self):
        return self.storage

    def set_state(self, state):
        self.storage = state


class APPOWithSharedDataActor(APPO):
    def setup(self, config: AlgorithmConfig):
        # Call to parent `setup`.
        super().setup(config)

        # Create shared data actor.
        self.shared_data_actor = SharedDataActor.remote()

        # Share the actor with all other relevant actors.

        def _share(actor, shared_act=self.shared_data_actor):
            actor._shared_data_actor = shared_act
            # Also add shared actor reference to all the learner connector pieces,
            # if applicable.
            if hasattr(actor, "_learner_connector") and actor._learner_connector:
                for conn in actor._learner_connector:
                    conn._shared_data_actor = shared_act

        self.env_runner_group.foreach_env_runner(func=_share)
        if self.eval_env_runner_group:
            self.eval_env_runner_group.foreach_env_runner(func=_share)
        self.learner_group.foreach_learner(func=_share)
        if self._aggregator_actor_manager:
            self._aggregator_actor_manager.foreach_actor(func=_share)

    def get_state(self, *args, **kwargs):
        state = super().get_state(*args, **kwargs)
        # Add shared actor's state.
        state["shared_data_actor"] = ray.get(self.shared_data_actor.get_state.remote())
        return state

    def set_state(self, state, *args, **kwargs):
        super().set_state(state, *args, **kwargs)
        # Set shared actor's state.
        if "shared_data_actor" in state:
            self.shared_data_actor.set_state.remote(state["shared_data_actor"])

    def restore_env_runners(self, env_runner_group: EnvRunnerGroup) -> List[int]:
        restored = super().restore_env_runners(env_runner_group)

        # For the restored EnvRunners, send them the latest shared, global state
        # from the `SharedDataActor`.
        for restored_idx in restored:
            state_ref = self.shared_data_actor.get.remote(
                key=f"EnvRunner_{restored_idx}"
            )
            env_runner_group.foreach_env_runner(
                lambda env_runner, state=state_ref: env_runner._global_state,
                remote_worker_ids=[restored_idx],
                timeout_seconds=0.0,
            )

        return restored

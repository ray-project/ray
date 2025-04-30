import ray
from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.algorithms.appo import APPO
from ray.rllib.env.env_runner_group import EnvRunnerGroup


@ray.remote
class SharedDataActor:
    def __init__(self):
        self.storage = {}

    def put(self, key, value):
        self.storage[key] = value

    def get(self, key):
        return self.storage.get(key)


class APPOWithSharedDataActor(APPO):
    def setup(self, config: AlgorithmConfig):
        # Call to parent `setup`.
        super().setup(config)

        # Create shared data actor.
        self.shared_data_actor = SharedDataActor.remote()

        # Share the actor with all other relevant actors.

        def _share(env_runner, shared_act=self.shared_data_actor):
            env_runner.shared_data_actor = shared_act

        self.env_runner_group.foreach_env_runner(func=_share)

    def get_state(self, *args, **kwargs):
        state = super().get_state(*args, **kwargs)
        # Add shared actor's state.
        state["shared_data_actor"] = self.shared_data_actor.get_state.remote()
        return state

    def set_state(self, state, *args, **kwargs):
        super().set_state(state, *args, **kwargs)
        # Set shared actor's state.
        if "shared_data_actor" in state:
            self.shared_data_actor.set_state.remote(state["shared_data_actor"])




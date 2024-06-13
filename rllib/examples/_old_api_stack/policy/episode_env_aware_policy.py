# @OldAPIStack
import numpy as np
import tree
from gymnasium.spaces import Box

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.examples._old_api_stack.policy.random_policy import RandomPolicy
from ray.rllib.examples.rl_modules.classes.random_rlm import StatefulRandomRLModule
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy


class StatefulRandomPolicy(RandomPolicy):
    """A Policy that has acts randomly and has stateful view requirements."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        config = RLModuleConfig(
            action_space=self.action_space,
            model_config_dict={
                "max_seq_len": 50,
                "lstm_use_prev_action": False,
                "lstm_use_prev_reward": False,
            },
        )
        self.model = StatefulRandomRLModule(config=config)

        self.view_requirements = self.model.update_default_view_requirements(
            self.view_requirements
        )

    @override(Policy)
    def is_recurrent(self):
        return True

    def get_initial_state(self):
        if self.config.get("enable_rl_module_and_learner", False):
            # convert the tree of tensors to a tree to numpy arrays
            return tree.map_structure(
                lambda s: convert_to_numpy(s), self.model.get_initial_state()
            )

    @override(Policy)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        sample_batch["2xobs"] = sample_batch["obs"] * 2.0
        return sample_batch

    @override(Policy)
    def compute_actions_from_input_dict(self, input_dict, *args, **kwargs):
        fwd_out = self.model.forward_exploration(input_dict)
        actions = fwd_out[SampleBatch.ACTIONS]
        state_out = fwd_out[Columns.STATE_OUT]
        return actions, state_out, {}


class EpisodeEnvAwareAttentionPolicy(RandomPolicy):
    """A Policy that always knows the current EpisodeID and EnvID and
    returns these in its actions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state_space = Box(-1.0, 1.0, (1,))
        self.config["model"] = {"max_seq_len": 50}

        class _fake_model:
            def __init__(self, state_space, config):
                self.state_space = state_space
                self.view_requirements = {
                    SampleBatch.AGENT_INDEX: ViewRequirement(),
                    SampleBatch.EPS_ID: ViewRequirement(),
                    "env_id": ViewRequirement(),
                    "t": ViewRequirement(),
                    SampleBatch.OBS: ViewRequirement(),
                    "state_in_0": ViewRequirement(
                        "state_out_0",
                        # Provide state outs -50 to -1 as "state-in".
                        shift="-50:-1",
                        # Repeat the incoming state every n time steps (usually max seq
                        # len).
                        batch_repeat_value=config["model"]["max_seq_len"],
                        space=state_space,
                    ),
                    "state_out_0": ViewRequirement(
                        space=state_space, used_for_compute_actions=False
                    ),
                }

            def compile(self, *args, **kwargs):
                """Dummy method for compatibility with TorchRLModule.

                This is hit when RolloutWorker tries to compile TorchRLModule."""
                pass

            # We need to provide at least an initial state if we want agent collector
            # to build episodes with state_in_ and state_out_
            def get_initial_state(self):
                return [self.state_space.sample()]

        self.model = _fake_model(self.state_space, self.config)

        self.view_requirements = dict(
            super()._get_default_view_requirements(), **self.model.view_requirements
        )

    @override(Policy)
    def get_initial_state(self):
        return self.model.get_initial_state()

    @override(Policy)
    def is_recurrent(self):
        return True

    @override(Policy)
    def compute_actions_from_input_dict(
        self, input_dict, explore=None, timestep=None, **kwargs
    ):
        ts = input_dict["t"]
        print(ts)
        # Always return [episodeID, envID] as actions.
        actions = np.array(
            [
                [
                    input_dict[SampleBatch.AGENT_INDEX][i],
                    input_dict[SampleBatch.EPS_ID][i],
                    input_dict["env_id"][i],
                ]
                for i, _ in enumerate(input_dict["obs"])
            ]
        )
        states = [np.array([[ts[i]] for i in range(len(input_dict["obs"]))])]
        self.global_timestep += 1
        return actions, states, {}

    @override(Policy)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        sample_batch["3xobs"] = sample_batch["obs"] * 3.0
        return sample_batch

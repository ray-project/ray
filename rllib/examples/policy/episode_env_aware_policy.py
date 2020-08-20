import numpy as np

from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override


class EpisodeEnvAwarePolicy(RandomPolicy):
    """A Policy that always knows the current EpisodeID and EnvID and
    returns these in its actions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.episode_id = None
        self.env_id = None

        class _fake_model:
            pass

        self.model = _fake_model()
        self.model.time_major = True
        self.model.inference_view_requirements = {
            SampleBatch.EPS_ID: ViewRequirement(),
            "env_id": ViewRequirement(),
            SampleBatch.OBS: ViewRequirement(),
            SampleBatch.PREV_ACTIONS: ViewRequirement(
                SampleBatch.ACTIONS, space=self.action_space, shift=-1),
            SampleBatch.PREV_REWARDS: ViewRequirement(
                SampleBatch.REWARDS, shift=-1),
        }
        self.training_view_requirements = dict(
            **{
                SampleBatch.NEXT_OBS: ViewRequirement(
                    SampleBatch.OBS, shift=1),
                SampleBatch.ACTIONS: ViewRequirement(space=self.action_space),
                SampleBatch.REWARDS: ViewRequirement(),
                SampleBatch.DONES: ViewRequirement(),
            },
            **self.model.inference_view_requirements)

    @override(Policy)
    def is_recurrent(self):
        return True

    @override(Policy)
    def compute_actions_from_input_dict(self,
                                        input_dict,
                                        explore=None,
                                        timestep=None,
                                        **kwargs):
        self.episode_id = input_dict[SampleBatch.EPS_ID][0]
        self.env_id = input_dict["env_id"][0]
        # Always return (episodeID, envID)
        return [
            np.array([self.episode_id, self.env_id]) for _ in input_dict["obs"]
        ], [], {}

    @override(Policy)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        sample_batch["postprocessed_column"] = sample_batch["obs"] + 1.0
        return sample_batch

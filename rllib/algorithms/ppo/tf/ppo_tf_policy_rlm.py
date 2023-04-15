from ray.rllib.algorithms.ppo.ppo_tf_policy import validate_config
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.utils.annotations import override


# TODO: Remove once we have a RLModule capable sampler class that can replace
#  `Policy.compute_actions_from_input_dict()`.
class PPOTfPolicyWithRLModule(EagerTFPolicyV2):
    def __init__(self, observation_space, action_space, config):
        validate_config(config)
        EagerTFPolicyV2.enable_eager_execution_if_necessary()
        self.framework = "tf2"
        EagerTFPolicyV2.__init__(self, observation_space, action_space, config)
        self.maybe_initialize_optimizer_and_loss()

    @override(EagerTFPolicyV2)
    def loss(self, model, dist_class, train_batch):
        self.stats = {}
        train_batch[SampleBatch.ACTION_LOGP]
        train_batch[SampleBatch.ACTIONS]
        train_batch[SampleBatch.REWARDS]
        train_batch[SampleBatch.TERMINATEDS]
        return 0.0

    @override(EagerTFPolicyV2)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        sample_batch = super().postprocess_trajectory(sample_batch)
        return compute_gae_for_sample_batch(
            self, sample_batch, other_agent_batches, episode
        )

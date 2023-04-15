from ray.rllib.algorithms.ppo.ppo_tf_policy import validate_config
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


# TODO: Remove once we have a RLModule capable sampler class that can replace
#  `Policy.compute_actions_from_input_dict()`.
class PPOTorchPolicyWithRLModule(TorchPolicyV2):
    def __init__(self, observation_space, action_space, config):
        validate_config(config)
        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )
        self._initialize_loss_from_dummy_batch()

    @override(TorchPolicyV2)
    def loss(self, model, dist_class, train_batch):
        self.stats = {}
        train_batch[SampleBatch.ACTION_LOGP]
        train_batch[SampleBatch.ACTIONS]
        train_batch[SampleBatch.REWARDS]
        train_batch[SampleBatch.TERMINATEDS]
        return 0.0

    @override(TorchPolicyV2)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        with torch.no_grad():
            return compute_gae_for_sample_batch(
                self, sample_batch, other_agent_batches, episode
            )

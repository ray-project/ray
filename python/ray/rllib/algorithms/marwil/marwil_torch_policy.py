from typing import Dict, List, Type, Union

from ray.rllib.algorithms.marwil.marwil_tf_policy import PostprocessAdvantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import ValueNetworkMixin
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import apply_grad_clipping, explained_variance
from ray.rllib.utils.typing import TensorType

torch, _ = try_import_torch()


class MARWILTorchPolicy(ValueNetworkMixin, PostprocessAdvantages, TorchPolicyV2):
    """PyTorch policy class used with Marwil."""

    def __init__(self, observation_space, action_space, config):
        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        ValueNetworkMixin.__init__(self, config)
        PostprocessAdvantages.__init__(self)

        # Not needed for pure BC.
        if config["beta"] != 0.0:
            # Set up a torch-var for the squared moving avg. advantage norm.
            self._moving_average_sqd_adv_norm = torch.tensor(
                [config["moving_average_sqd_adv_norm_start"]],
                dtype=torch.float32,
                requires_grad=False,
            ).to(self.device)

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        model_out, _ = model(train_batch)
        action_dist = dist_class(model_out, model)
        actions = train_batch[SampleBatch.ACTIONS]
        # log\pi_\theta(a|s)
        logprobs = action_dist.logp(actions)

        # Advantage estimation.
        if self.config["beta"] != 0.0:
            cumulative_rewards = train_batch[Postprocessing.ADVANTAGES]
            state_values = model.value_function()
            adv = cumulative_rewards - state_values
            adv_squared_mean = torch.mean(torch.pow(adv, 2.0))

            explained_var = explained_variance(cumulative_rewards, state_values)
            ev = torch.mean(explained_var)
            model.tower_stats["explained_variance"] = ev

            # Policy loss.
            # Update averaged advantage norm.
            rate = self.config["moving_average_sqd_adv_norm_update_rate"]
            self._moving_average_sqd_adv_norm = (
                rate * (adv_squared_mean.detach() - self._moving_average_sqd_adv_norm)
                + self._moving_average_sqd_adv_norm
            )
            model.tower_stats[
                "_moving_average_sqd_adv_norm"
            ] = self._moving_average_sqd_adv_norm
            # Exponentially weighted advantages.
            exp_advs = torch.exp(
                self.config["beta"]
                * (adv / (1e-8 + torch.pow(self._moving_average_sqd_adv_norm, 0.5)))
            ).detach()
            # Value loss.
            v_loss = 0.5 * adv_squared_mean
        else:
            # Policy loss (simple BC loss term).
            exp_advs = 1.0
            # Value loss.
            v_loss = 0.0
        model.tower_stats["v_loss"] = v_loss
        # logprob loss alone tends to push action distributions to
        # have very low entropy, resulting in worse performance for
        # unfamiliar situations.
        # A scaled logstd loss term encourages stochasticity, thus
        # alleviate the problem to some extent.
        logstd_coeff = self.config["bc_logstd_coeff"]
        if logstd_coeff > 0.0:
            logstds = torch.mean(action_dist.log_std, dim=1)
        else:
            logstds = 0.0

        p_loss = -torch.mean(exp_advs * (logprobs + logstd_coeff * logstds))
        model.tower_stats["p_loss"] = p_loss
        # Combine both losses.
        self.v_loss = v_loss
        self.p_loss = p_loss
        total_loss = p_loss + self.config["vf_coeff"] * v_loss
        model.tower_stats["total_loss"] = total_loss
        return total_loss

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        stats = {
            "policy_loss": self.get_tower_stats("p_loss")[0].item(),
            "total_loss": self.get_tower_stats("total_loss")[0].item(),
        }
        if self.config["beta"] != 0.0:
            stats["moving_average_sqd_adv_norm"] = self.get_tower_stats(
                "_moving_average_sqd_adv_norm"
            )[0].item()
            stats["vf_explained_var"] = self.get_tower_stats("explained_variance")[
                0
            ].item()
            stats["vf_loss"] = self.get_tower_stats("v_loss")[0].item()
        return convert_to_numpy(stats)

    def extra_grad_process(
        self, optimizer: "torch.optim.Optimizer", loss: TensorType
    ) -> Dict[str, TensorType]:
        return apply_grad_clipping(self, optimizer, loss)

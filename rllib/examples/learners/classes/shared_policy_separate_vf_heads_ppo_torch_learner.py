from typing import Any, Dict, List


from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_KL_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY,
)
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.connectors.common import NumpyToTensor
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.torch_utils import explained_variance
from ray.rllib.utils.typing import EpisodeType, ModuleID, TensorType

torch, _ = try_import_torch()


class SharedPolicySeparateVFHeadsPPOTorchLearner(PPOTorchLearner):
    """A custom PPO torch learner for a global policy and many vf-heads.

    The global policy takes a global observation as input and outputs all agents'
    actions at once, for example as parameters for a `MultiDiscrete` distribution in
    case the n agents each have a Discrete action space.

    The n value function heads are trained through individual reward streams and thus
    individual GAE computations, meaning one advantage stream and loss term per agent.
    """

    def build(self):
        super().build()

        # Remove PPO's default GAE connector and inject our own per-agent GAE
        # connector.
        self._learner_connector.remove("GeneralAdvantageEstimation")
        self._learner_connector.append(
            _GAEForModuleWithMultipleVFHeads(
                gamma=self.config.gamma,
                lambda_=self.config.lambda_,
            )
        )

    @override(PPOTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: PPOConfig,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        assert module_id == "global"

        module = self.module[module_id].unwrapped()

        # Possibly apply masking to some sub loss terms and to the total loss term
        # at the end. Masking could be used for RNN-based model (zero padded `batch`)
        # and for PPO's batched value function (and bootstrap value) computations,
        # for which we add an (artificial) timestep to each episode to
        # simplify the actual computation.
        if Columns.LOSS_MASK in batch:
            mask = batch[Columns.LOSS_MASK]
            num_valid = torch.sum(mask)

            def possibly_masked_mean(data_):
                return torch.sum(data_[mask]) / num_valid

        else:
            possibly_masked_mean = torch.mean

        # Split actions into n individual agents' actions.
        action_batches = [
            b.squeeze(-1) for b in torch.split(batch[Columns.ACTIONS], 1, dim=-1)
        ]
        action_dist_inputs_fwd = torch.split(
            fwd_out[Columns.ACTION_DIST_INPUTS], 2, dim=-1
        )
        action_dist_inputs_batch = torch.split(
            batch[Columns.ACTION_DIST_INPUTS], 2, dim=-1
        )
        action_dist_class = TorchCategorical

        value_fn_out = None
        if config.use_critic:
            value_fn_out = module.compute_values(
                batch, embeddings=fwd_out.get(Columns.EMBEDDINGS)
            )

        total_loss = 0.0
        for agent in [0, 1]:
            key = f"_agent{agent}"
            curr_action_dist = action_dist_class.from_logits(
                action_dist_inputs_fwd[agent]
            )
            # TODO (sven): We should ideally do this in the LearnerConnector (separation of
            #  concerns: Only do things on the EnvRunners that are required for computing
            #  actions, do NOT do anything on the EnvRunners that's only required for a
            #   training update).
            prev_action_dist = action_dist_class.from_logits(
                action_dist_inputs_batch[agent]
            )
            prev_action_logp = prev_action_dist.logp(action_batches[agent])

            logp_ratio = torch.exp(
                curr_action_dist.logp(action_batches[agent]) - prev_action_logp
            )

            # Only calculate kl loss if necessary (kl-coeff > 0.0).
            if config.use_kl_loss:
                action_kl = prev_action_dist.kl(curr_action_dist)
                mean_kl_loss = possibly_masked_mean(action_kl)
            else:
                mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

            curr_entropy = curr_action_dist.entropy()
            mean_entropy = possibly_masked_mean(curr_entropy)

            surrogate_loss = torch.min(
                batch[Columns.ADVANTAGES + key] * logp_ratio,
                batch[Columns.ADVANTAGES + key]
                * torch.clamp(logp_ratio, 1 - config.clip_param, 1 + config.clip_param),
            )

            # Compute a value function loss.
            if config.use_critic:
                vf_loss = torch.pow(
                    value_fn_out[:, agent] - batch[Columns.VALUE_TARGETS + key], 2.0
                )
                vf_loss_clipped = torch.clamp(vf_loss, 0, config.vf_clip_param)
                mean_vf_loss = possibly_masked_mean(vf_loss_clipped)
                mean_vf_unclipped_loss = possibly_masked_mean(vf_loss)
            # Ignore the value function -> Set all to 0.0.
            else:
                z = torch.tensor(0.0, device=surrogate_loss.device)
                value_fn_out = (
                    mean_vf_unclipped_loss
                ) = vf_loss_clipped = mean_vf_loss = z

            total_loss += possibly_masked_mean(
                -surrogate_loss
                + config.vf_loss_coeff * vf_loss_clipped
                - (
                    self.entropy_coeff_schedulers_per_module[
                        module_id
                    ].get_current_value()
                    * curr_entropy
                )
            )

            # Add mean_kl_loss (already processed through `possibly_masked_mean`),
            # if necessary.
            if config.use_kl_loss:
                total_loss += self.curr_kl_coeffs_per_module[module_id] * mean_kl_loss

            # Log important loss stats.
            self.metrics.log_dict(
                {
                    POLICY_LOSS_KEY: -possibly_masked_mean(surrogate_loss),
                    VF_LOSS_KEY: mean_vf_loss,
                    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: mean_vf_unclipped_loss,
                    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                        batch[Columns.VALUE_TARGETS + key], value_fn_out[:, agent]
                    ),
                    ENTROPY_KEY: mean_entropy,
                    LEARNER_RESULTS_KL_KEY: mean_kl_loss,
                },
                key=(module_id, key),
                window=1,  # <- single items (should not be mean/ema-reduced over time).
            )
        # Return the total loss.
        return total_loss


class _GAEForModuleWithMultipleVFHeads(ConnectorV2):
    """Custom LearnerConnector replacing PPO's default GAE one.

    Used by the above custom Learner class' loss function, namely in the case where
    the policy network is shared across all agents outputting a composite action, for
    example multi-discrete, and there are n separate value functions or heads, each of
    which requires its separate GAE computation from the agents' individual rewards.

    Note that individual rewards must be present in the train batch under the columns
    "rewards_agent[idx]", where `idx` is an integer starting with 0.
    """

    def __init__(
        self,
        input_observation_space=None,
        input_action_space=None,
        *,
        gamma,
        lambda_,
    ):
        super().__init__(input_observation_space, input_action_space)
        self.gamma = gamma
        self.lambda_ = lambda_

        # Internal numpy-to-tensor connector to translate GAE results (advantages and
        # vf targets) into tensors.
        self._numpy_to_tensor_connector = None

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: MultiRLModule,
        episodes: List[EpisodeType],
        batch: Dict[str, Any],
        **kwargs,
    ):
        # Device to place all GAE result tensors (advantages and value targets) on.
        device = None

        # Extract all single-agent episodes.
        sa_episodes_list = list(
            self.single_agent_episode_iterator(episodes, agents_that_stepped_only=False)
        )
        # Perform the value nets' forward passes.
        vf_preds = rl_module.foreach_module(
            func=lambda mid, module: (
                module.compute_values(batch[mid])
                if mid in batch and isinstance(module, ValueFunctionAPI)
                else None
            ),
            return_dict=True,
        )
        # Loop through all modules and perform each one's GAE computation.
        for module_id, module_vf_preds in vf_preds.items():
            if module_vf_preds is None:
                continue
            assert not rl_module[module_id].is_stateful()

            device = module_vf_preds.device
            # Convert to numpy for the upcoming GAE computations.
            module_vf_preds = convert_to_numpy(module_vf_preds)

            # Collect (single-agent) episode lengths for this particular module.
            episode_lens = [
                len(e) for e in sa_episodes_list if e.module_id in [None, module_id]
            ]
            # Loop through all agents' predicted values and compute value targets.
            for agent_idx in range(module_vf_preds.shape[-1]):
                agent_rewards = batch[module_id][Columns.REWARDS + f"_agent{agent_idx}"]
                agent_vf_preds = module_vf_preds[:, agent_idx]
                agent_value_targets = compute_value_targets(
                    values=agent_vf_preds,
                    rewards=convert_to_numpy(agent_rewards),
                    terminateds=convert_to_numpy(batch[module_id][Columns.TERMINATEDS]),
                    truncateds=convert_to_numpy(batch[module_id][Columns.TRUNCATEDS]),
                    gamma=self.gamma,
                    lambda_=self.lambda_,
                )
                assert agent_value_targets.shape[0] == sum(episode_lens)

                agent_advantages = agent_value_targets - agent_vf_preds
                # Drop vf-preds, not needed in loss. Note that in the DefaultPPORLModule,
                # vf-preds are recomputed with each `forward_train` call anyway to compute
                # the vf loss.
                # Standardize advantages (used for more stable and better weighted
                # policy gradient computations).
                agent_advantages = (agent_advantages - agent_advantages.mean()) / max(
                    1e-4, agent_advantages.std()
                )
                batch[module_id][
                    Columns.ADVANTAGES + f"_agent{agent_idx}"
                ] = agent_advantages
                batch[module_id][
                    Columns.VALUE_TARGETS + f"_agent{agent_idx}"
                ] = agent_value_targets

        # Convert all GAE results to tensors.
        if self._numpy_to_tensor_connector is None:
            self._numpy_to_tensor_connector = NumpyToTensor(
                as_learner_connector=True, device=device
            )
        tensor_results = self._numpy_to_tensor_connector(
            rl_module=rl_module,
            batch={
                mid: {
                    Columns.ADVANTAGES
                    + "_agent0": (module_batch[Columns.ADVANTAGES + "_agent0"]),
                    Columns.ADVANTAGES
                    + "_agent1": (module_batch[Columns.ADVANTAGES + "_agent1"]),
                    Columns.VALUE_TARGETS
                    + "_agent0": (module_batch[Columns.VALUE_TARGETS + "_agent0"]),
                    Columns.VALUE_TARGETS
                    + "_agent1": (module_batch[Columns.VALUE_TARGETS + "_agent1"]),
                }
                for mid, module_batch in batch.items()
                if vf_preds[mid] is not None
            },
            episodes=episodes,
        )
        # Move converted tensors back to `batch`.
        for mid, module_batch in tensor_results.items():
            batch[mid].update(module_batch)

        return batch

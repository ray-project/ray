import abc
from typing import Any, Dict, List, Optional, Tuple, Union

from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
    PPOConfig,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import Learner
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.episodes import (
    add_one_ts_to_episodes_and_truncate,
    remove_last_ts_from_data,
    remove_last_ts_from_episodes_and_restore_truncateds,
)
from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import EpisodeType, ModuleID, TensorType


class PPOLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Dict mapping module IDs to the respective entropy Scheduler instance.
        self.entropy_coeff_schedulers_per_module: Dict[
            ModuleID, Scheduler
        ] = LambdaDefaultDict(
            lambda module_id: Scheduler(
                fixed_value_or_schedule=(
                    self.config.get_config_for_module(module_id).entropy_coeff
                ),
                framework=self.framework,
                device=self._device,
            )
        )

        # Set up KL coefficient variables (per module).
        # Note that the KL coeff is not controlled by a Scheduler, but seeks
        # to stay close to a given kl_target value.
        self.curr_kl_coeffs_per_module: Dict[ModuleID, TensorType] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                self.config.get_config_for_module(module_id).kl_coeff
            )
        )

    @override(Learner)
    def _update_from_batch_or_episodes(
        self,
        *,
        batch=None,
        episodes=None,
        **kwargs,
    ):
        # First perform GAE computation on the entirety of the given train data (all
        # episodes).
        if self.config.enable_env_runner_and_connector_v2:
            batch, episodes = self._compute_gae_from_episodes(episodes=episodes)

        # Now that GAE (advantages and value targets) have been added to the train
        # batch, we can proceed normally (calling super method) with the update step.
        return super()._update_from_batch_or_episodes(
            batch=batch,
            episodes=episodes,
            **kwargs,
        )

    def _compute_gae_from_episodes(
        self,
        *,
        episodes: Optional[List[EpisodeType]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[List[EpisodeType]]]:
        """Computes GAE advantages (and value targets) given a list of episodes.

        Note that the episodes may be SingleAgent- or MultiAgentEpisodes and may be
        episode chunks (not starting from reset or ending prematurely).

        The GAE computation here is performed in a very efficient way via elongating
        all given episodes by 1 artificial timestep (last obs, actions, states, etc..
        repeated, last reward=0.0, etc..), then creating a forward batch from this data
        using the connector, pushing the resulting batch through the value function,
        thereby extracting the bootstrap values (at the artificially added time steos)
        and all other value predictions (all other timesteps) and then reducing the
        batch and episode lengths again accordingly.
        """
        if not episodes:
            raise ValueError(
                "`PPOLearner._compute_gae_from_episodes()` must have the `episodes` "
                "arg provided! Otherwise, GAE/advantage computation can't be performed."
            )

        batch = {}

        sa_episodes_list = list(
            self._learner_connector.single_agent_episode_iterator(
                episodes, agents_that_stepped_only=False
            )
        )
        # Make all episodes one ts longer in order to just have a single batch
        # (and distributed forward pass) for both vf predictions AND the bootstrap
        # vf computations.
        orig_truncateds_of_sa_episodes = add_one_ts_to_episodes_and_truncate(
            sa_episodes_list
        )

        # Call the learner connector (on the artificially elongated episodes)
        # in order to get the batch to pass through the module for vf (and
        # bootstrapped vf) computations.
        batch_for_vf = self._learner_connector(
            rl_module=self.module,
            data={},
            episodes=episodes,
            shared_data={},
        )
        # Perform the value model's forward pass.
        vf_preds = convert_to_numpy(self._compute_values(batch_for_vf))

        for module_id, module_vf_preds in vf_preds.items():
            # Collect new (single-agent) episode lengths.
            episode_lens_plus_1 = [
                len(e)
                for e in sa_episodes_list
                if e.module_id is None or e.module_id == module_id
            ]

            # Remove all zero-padding again, if applicable, for the upcoming
            # GAE computations.
            module_vf_preds = unpad_data_if_necessary(
                episode_lens_plus_1, module_vf_preds
            )
            # Compute value targets.
            module_value_targets = compute_value_targets(
                values=module_vf_preds,
                rewards=unpad_data_if_necessary(
                    episode_lens_plus_1,
                    convert_to_numpy(batch_for_vf[module_id][Columns.REWARDS]),
                ),
                terminateds=unpad_data_if_necessary(
                    episode_lens_plus_1,
                    convert_to_numpy(batch_for_vf[module_id][Columns.TERMINATEDS]),
                ),
                truncateds=unpad_data_if_necessary(
                    episode_lens_plus_1,
                    convert_to_numpy(batch_for_vf[module_id][Columns.TRUNCATEDS]),
                ),
                gamma=self.config.gamma,
                lambda_=self.config.lambda_,
            )

            # Remove the extra timesteps again from vf_preds and value targets. Now that
            # the GAE computation is done, we don't need this last timestep anymore in
            # any of our data.
            module_vf_preds, module_value_targets = remove_last_ts_from_data(
                episode_lens_plus_1,
                module_vf_preds,
                module_value_targets,
            )
            module_advantages = module_value_targets - module_vf_preds
            # Drop vf-preds, not needed in loss. Note that in the PPORLModule, vf-preds
            # are recomputed with each `forward_train` call anyway.
            # Standardize advantages (used for more stable and better weighted
            # policy gradient computations).
            module_advantages = (module_advantages - module_advantages.mean()) / max(
                1e-4, module_advantages.std()
            )

            # Restructure ADVANTAGES and VALUE_TARGETS in a way that the Learner
            # connector can properly re-batch these new fields.
            batch_pos = 0
            for eps in sa_episodes_list:
                if eps.module_id is not None and eps.module_id != module_id:
                    continue
                len_ = len(eps) - 1
                self._learner_connector.add_n_batch_items(
                    batch=batch,
                    column=Postprocessing.ADVANTAGES,
                    items_to_add=module_advantages[batch_pos : batch_pos + len_],
                    num_items=len_,
                    single_agent_episode=eps,
                )
                self._learner_connector.add_n_batch_items(
                    batch=batch,
                    column=Postprocessing.VALUE_TARGETS,
                    items_to_add=module_value_targets[batch_pos : batch_pos + len_],
                    num_items=len_,
                    single_agent_episode=eps,
                )
                batch_pos += len_

        # Remove the extra (artificial) timesteps again at the end of all episodes.
        remove_last_ts_from_episodes_and_restore_truncateds(
            sa_episodes_list,
            orig_truncateds_of_sa_episodes,
        )

        return batch, episodes

    @override(Learner)
    def remove_module(self, module_id: ModuleID, **kwargs):
        marl_spec = super().remove_module(module_id, **kwargs)

        self.entropy_coeff_schedulers_per_module.pop(module_id, None)
        self.curr_kl_coeffs_per_module.pop(module_id, None)

        return marl_spec

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(Learner)
    def after_gradient_based_update(
        self,
        *,
        timesteps: Dict[str, Any],
    ) -> None:
        super().after_gradient_based_update(timesteps=timesteps)

        for module_id, module in self.module._rl_modules.items():
            config = self.config.get_config_for_module(module_id)

            # Update entropy coefficient via our Scheduler.
            new_entropy_coeff = self.entropy_coeff_schedulers_per_module[
                module_id
            ].update(timestep=timesteps.get(NUM_ENV_STEPS_SAMPLED_LIFETIME, 0))
            self.metrics.log_value(
                (module_id, LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY),
                new_entropy_coeff,
                window=1,
            )
            if (
                config.use_kl_loss
                and self.metrics.peek((module_id, NUM_MODULE_STEPS_TRAINED), default=0)
                > 0
            ):
                self._update_module_kl_coeff(module_id=module_id, config=config)

    @OverrideToImplementCustomLogic
    def _compute_values(
        self,
        batch_for_vf: Dict[str, Any],
    ) -> Union[TensorType, Dict[str, Any]]:
        """Computes the value function predictions for the module being optimized.

        This method must be overridden by multiagent-specific algorithm learners to
        specify the specific value computation logic. If the algorithm is single agent
        (or independent multi-agent), there should be no need to override this method.

        Args:
            batch_for_vf: The multi-agent batch (mapping ModuleIDs to module data) to
                be used for value function predictions.

        Returns:
            A dictionary mapping module IDs to individual value function prediction
            tensors.
        """
        return {
            module_id: self.module[module_id].unwrapped().compute_values(module_batch)
            for module_id, module_batch in batch_for_vf.items()
            if self.should_module_be_updated(module_id, batch_for_vf)
        }

    @abc.abstractmethod
    def _update_module_kl_coeff(
        self,
        *,
        module_id: ModuleID,
        config: PPOConfig,
    ) -> None:
        """Dynamically update the KL loss coefficients of each module with.

        The update is completed using the mean KL divergence between the action
        distributions current policy and old policy of each module. That action
        distribution is computed during the most recent update/call to `compute_loss`.

        Args:
            module_id: The module whose KL loss coefficient to update.
            config: The AlgorithmConfig specific to the given `module_id`.
        """

from typing import Any, List, Dict, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.postprocessing.value_predictions import compute_value_targets
from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary
from ray.rllib.utils.typing import EpisodeType


class GeneralAdvantageEstimation(ConnectorV2):
    def __init__(self, input_observation_space=None, input_action_space=None, *, gamma, lambda_):
        super().__init__(input_observation_space, input_action_space)
        self.gamma = gamma
        self.lambda_ = lambda_

    def __call__(
        self,
        *,
        rl_module: RLModule,
        episodes: List[EpisodeType],
        batch: Dict[str, Any],
        **kwargs,
    ):
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
        sa_episodes_list = list(
            self.single_agent_episode_iterator(
                episodes, agents_that_stepped_only=False
            )
        )
        ## Make all episodes one ts longer in order to just have a single batch
        ## (and distributed forward pass) for both vf predictions AND the bootstrap
        ## vf computations.
        #orig_truncateds_of_sa_episodes = add_one_ts_to_episodes_and_truncate(
        #    sa_episodes_list
        #)

        # Call the learner connector (on the artificially elongated episodes)
        # in order to get the batch to pass through the module for vf (and
        # bootstrapped vf) computations.
        #batch_for_vf = self._learner_connector(
        #    rl_module=self.module,
        #    batch={},
        #    episodes=episodes,
        #    shared_data={},
        #)
        # Perform the value model's forward pass.
        vf_preds = convert_to_numpy(rl_module.foreach_module(
            func=lambda mid, module: (
                module.compute_values(batch[mid])
                if isinstance(module, ValueFunctionAPI)
                else None
            ),
            return_dict=True,
        ))
        #assert sum(map(len, sa_episodes_list)) == vf_preds

        for module_id, module_vf_preds in vf_preds.items():
            # Skip those outputs of RLModules that are not implementers of
            # `ValueFunctionAPI`.
            if module_vf_preds is None:
                continue

            # Collect (single-agent) episode lengths.
            episode_lens = [
                len(e)
                for e in sa_episodes_list
                if e.module_id is None or e.module_id == module_id
            ]

            # Remove all zero-padding again, if applicable, for the upcoming
            # GAE computations.
            module_vf_preds = unpad_data_if_necessary(episode_lens, module_vf_preds)
            # Compute value targets.
            module_value_targets = compute_value_targets(
                values=module_vf_preds,
                rewards=unpad_data_if_necessary(
                    episode_lens,
                    convert_to_numpy(batch[module_id][Columns.REWARDS]),
                ),
                terminateds=unpad_data_if_necessary(
                    episode_lens,
                    convert_to_numpy(batch[module_id][Columns.TERMINATEDS]),
                ),
                truncateds=unpad_data_if_necessary(
                    episode_lens,
                    convert_to_numpy(batch[module_id][Columns.TRUNCATEDS]),
                ),
                gamma=self.gamma,
                lambda_=self.lambda_,
            )

            # Remove the extra timesteps again from vf_preds and value targets. Now that
            # the GAE computation is done, we don't need this last timestep anymore in
            # any of our data.
            #module_vf_preds, module_value_targets = remove_last_ts_from_data(
            #    episode_lens_plus_1,
            #    module_vf_preds,
            #    module_value_targets,
            #)
            module_advantages = module_value_targets - module_vf_preds
            # Drop vf-preds, not needed in loss. Note that in the PPORLModule, vf-preds
            # are recomputed with each `forward_train` call anyway.
            # Standardize advantages (used for more stable and better weighted
            # policy gradient computations).
            module_advantages = (module_advantages - module_advantages.mean()) / max(
                1e-4, module_advantages.std()
            )

            TODO: change add_batch_item to throwing error if batch already in module-major format(
                then, simply do this here: batch[module_id][advantages] = module_advantages
                and batch[module_id][value_targets] = module_value_targets

            # Restructure ADVANTAGES and VALUE_TARGETS in a way that the Learner
            # connector can properly re-batch these new fields.
            #for eps in sa_episodes_list:
            #    if eps.module_id is not None and eps.module_id != module_id:
            #        continue
            #    #len_ = len(eps)

            #    self.add_n_batch_items(
            #        batch=batch,
            #        column=Postprocessing.ADVANTAGES,
            #        items_to_add=module_advantages[batch_pos: batch_pos + len_],
            #        num_items=len_,
            #        single_agent_episode=eps,
            #    )
            #    self.add_n_batch_items(
            #        batch=batch,
            #        column=Postprocessing.VALUE_TARGETS,
            #        items_to_add=module_value_targets[batch_pos: batch_pos + len_],
            #        num_items=len_,
            #        single_agent_episode=eps,
            #    )
            #    batch_pos += len_

        # TODO: Call:
        #BatchIndividualItems
        # but only on the newly added columns.

        ## Remove the extra (artificial) timesteps again at the end of all episodes.
        #remove_last_ts_from_episodes_and_restore_truncateds(
        #    sa_episodes_list,
        #    orig_truncateds_of_sa_episodes,
        #)

        return batch, episodes

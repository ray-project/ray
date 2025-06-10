"""TODO:

"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent import (
    two_agent_cartpole_with_global_observations as global_obs_cartpole
)
from ray.rllib.examples.learners.classes.ppo_global_observations_many_vf_learner import (  # noqa
    PPOGlobalObservationsManyVF
)
from ray.rllib.examples.rl_modules.classes.global_observations_many_vf_heads_rlm import (  # noqa
    GlobalObsManyVFHeadsRLModule
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


class RewardsFromInfosConnector(ConnectorV2):
    def __call__(
        self,
        *,
        rl_module,
        batch,
        episodes,
        explore=None,
        shared_data=None,
        metrics=None,
        **kwargs,
    ):
        for sa_episode in self.single_agent_episode_iterator(episodes):
            infos = sa_episode.get_infos()[1:]
            for agent in [0, 1]:
                col = Columns.REWARDS + f"_agent{agent}"
                self.add_n_batch_items(
                    batch=batch,
                    column=col,
                    items_to_add=[info[col] for info in infos],
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
        return batch


parser = add_rllib_example_script_args(
    default_iters=200,
    default_reward=350.0,
    default_timesteps=200000,
)
parser.set_defaults(
    enable_new_api_stack=True,
)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        PPOConfig()
        .environment(env=global_obs_cartpole.TwoAgentCartPoleWithGlobalObservations)
        .training(
            learner_class=PPOGlobalObservationsManyVF,
            learner_connector=lambda obs_space, act_space: RewardsFromInfosConnector(),
            train_batch_size_per_learner=2000,
            lr=0.0001,
            num_epochs=6,
            vf_loss_coeff=1.0,
        )
        .multi_agent(
            # Use a simple set of policy IDs. Spaces for the individual policies
            # are inferred automatically using reverse lookup via the
            # `policy_mapping_fn` and the env provided spaces for the different
            # agents. Alternatively, you could use:
            # policies: {main0: PolicySpec(...), main1: PolicySpec}
            policies={"global"},
            # Simple mapping fn, mapping agent0 to main0 and agent1 to main1.
            policy_mapping_fn=(lambda aid, episode, **kw: "global"),
        )
        .rl_module(
            rl_module_spec=RLModuleSpec(
                module_class=GlobalObsManyVFHeadsRLModule,
                model_config={
                    "hidden_dims": [256, 256],
                }
            )
        )
    )

    run_rllib_example_script_experiment(base_config, args)

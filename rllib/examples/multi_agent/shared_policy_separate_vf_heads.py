"""Example for running RLlib with "shared-policy, separate vf-heads" multi-agent setup.

TODO: Sven

The env used in this example is a 2-agent cartpole env, where both agents are hidden
by the env behind a single "global" agent, because they share the same observations
and observation space. A global `MultiDiscrete([2, 2])` action is passed into the Env's
`step()` method and each component of that action represents an individual action for
one of the two agents.



How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --stop-reward=350.0`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

You can visualize experiment results in ~/ray_results using TensorBoard.


Results to expect
-----------------

For the training step - depending on your `--stop-reward` setting, you should see
something similar to this:

Number of trials: 1/1 (1 TERMINATED)
+--------------------------------------------------------+------------+
| Trial name                                             | status     |
|                                                        |            |
|--------------------------------------------------------+------------+
| PPO_TwoAgentCartPoleWithGlobalObservations_4a523_00000 | TERMINATED |
+--------------------------------------------------------+------------+
+--------+------------------+------------------------+------------------------+
|   iter |   total time (s) |    episode_return_mean |   num_env_steps_sample |
|        |                  |                        |             d_lifetime |
+--------+------------------+------------------------+------------------------|
|     56 |          40.8958 |                 367.06 |                 112000 |
+--------+------------------+------------------------+------------------------+


"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent import (
    two_agent_cartpole_with_global_observations as global_obs_cartpole,
)
from ray.rllib.examples.learners.classes import (
    shared_policy_separate_vf_heads_ppo_torch_learner as custom_learner_module,
)
from ray.rllib.examples.rl_modules.classes import (
    shared_policy_separate_vf_heads_rlm as custom_rlm_module,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


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
            # Plug in the custom Learner class (with custom loss).
            # Note that this Learner class automatically flips out the default PPO
            # GAE connector with its own version of it, which can handle the separate
            # value function heads within the same RLModule ("global").
            learner_class=(
                custom_learner_module.SharedPolicySeparateVFHeadsPPOTorchLearner
            ),
            # Plug in an additional custom Learner connector, converting the individual
            # rewards for each agent found in the `info` dicts into their own train
            # batch columns.
            learner_connector=(
                lambda obs_space, act_space: (
                    global_obs_cartpole.RewardsFromInfosConnector()
                )
            ),
            train_batch_size_per_learner=2000,
            lr=0.0003,
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
            # Plug in the custom module with a shared policy net and n value heads,
            # one for each agent.
            rl_module_spec=RLModuleSpec(
                module_class=custom_rlm_module.SharedPolicySeparateVFHeadsRLModule,
                model_config={
                    "hidden_dims": [256, 256],
                },
            )
        )
    )

    run_rllib_example_script_experiment(base_config, args)

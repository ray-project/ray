import functools

import torch.nn.init
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.prev_action_prev_reward import (
    PrevRewardPrevActionEnvToModule,
)
from ray.rllib.connectors.learner.prev_action_prev_reward import (
    PrevRewardPrevActionLearner,
)
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.env.multi_agent import MultiAgentStatelessCartPole
from ray.rllib.examples.rl_module.lstm_w_prev_actions_rewards_rlm import (
    TorchLSTMwPrevRewardsActionsRLM
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune import register_env


parser = add_rllib_example_script_args(
    default_reward=400.0, default_timesteps=1000000, default_iters=2000
)
parser.add_argument("--n-prev-rewards", type=int, default=1)
parser.add_argument("--n-prev-actions", type=int, default=1)


if __name__ == "__main__":
    args = parser.parse_args()

    # Define our custom connector pipelines.
    def _env_to_module(env):
        obs = (
            env.single_observation_space if args.num_agents == 0
            else env.observation_space
        )
        act = (
            env.single_action_space if args.num_agents == 0
            else env.action_space
        )
        # Create the env-to-module connector. We return an individual connector piece
        # here, which RLlib will then automatically integrate into a pipeline (and
        # add its default connector piece to the end of that pipeline).
        return PrevRewardPrevActionEnvToModule(
            obs,
            act,
            n_prev_rewards=args.n_prev_rewards,
            n_prev_actions=args.n_prev_actions,
        )

    def _learner_connector(input_observation_space, input_action_space):
        # Create the learner connector.
        return PrevRewardPrevActionLearner(
            input_observation_space,
            input_action_space,
            n_prev_rewards=args.n_prev_rewards,
            n_prev_actions=args.n_prev_actions,
        )

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentStatelessCartPole(
                config={"num_agents": args.num_agents}
            ),
        )
    else:
        register_env("env", lambda _: StatelessCartPole())

    rlm_spec = SingleAgentRLModuleSpec(module_class=TorchLSTMwPrevRewardsActionsRLM)

    config = (
        PPOConfig()
        # Use new API stack.
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .environment("env")
        .rl_module(rl_module_spec=rlm_spec)
        # And new EnvRunner.
        .rollouts(
            env_to_module_connector=_env_to_module,
            # Setup the correct env-runner to use depending on
            # old-stack/new-stack and multi-agent settings.
            env_runner_cls=(
                None if not args.enable_new_api_stack
                else SingleAgentEnvRunner if args.num_agents == 0
                else MultiAgentEnvRunner
            ),
        )
        .resources(
            num_learner_workers=0,
        )
        .training(
            learner_connector=_learner_connector,
            num_sgd_iter=5,
            vf_loss_coeff=0.0001,
            train_batch_size=512,
            model=dict({
                "use_lstm": True,
                "lstm_cell_size": 32,
                "fcnet_weights_initializer": torch.nn.init.xavier_uniform_,
                "fcnet_bias_initializer": (
                    functools.partial(torch.nn.init.constant_, val=0.0)
                ),
                "vf_share_layers": True,
                "uses_new_env_runners": True,
            }, **(
                {} if not args.enable_new_api_stack
                else {"uses_new_env_runners": True}
            )),
        )
    )

    run_rllib_example_script_experiment(config, args)

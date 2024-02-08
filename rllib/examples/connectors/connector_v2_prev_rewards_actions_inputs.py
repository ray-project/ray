import argparse
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
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.rl_module.lstm_w_prev_actions_rewards_rlm import (
    TorchLSTMwPrevRewardsActionsRLM
)
from ray.rllib.utils.test_utils import check_learning_achieved


parser = argparse.ArgumentParser()
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument("--n-prev-rewards", type=int, default=1)
parser.add_argument("--n-prev-actions", type=int, default=1)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=2000, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=1000000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=400.0, help="Reward at which we stop training."
)


if __name__ == "__main__":
    import ray
    from ray import air, tune

    args = parser.parse_args()

    ray.init()

    # Define our custom connector pipelines.
    def _env_to_module(env):
        # Create the env-to-module connector. We return an individual connector piece
        # here, which RLlib will then automatically integrate into a pipeline (and
        # add its default connector piece to the end of that pipeline).
        return PrevRewardPrevActionEnvToModule(
            input_observation_space=env.single_observation_space,
            input_action_space=env.single_action_space,
            n_prev_rewards=args.n_prev_rewards,
            n_prev_actions=args.n_prev_actions,
        )

    def _learner_connector(input_observation_space, input_action_space):
        # Create the learner connector.
        return PrevRewardPrevActionLearner(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            n_prev_rewards=args.n_prev_rewards,
            n_prev_actions=args.n_prev_actions,
        )

    rlm_spec = SingleAgentRLModuleSpec(module_class=TorchLSTMwPrevRewardsActionsRLM)

    config = (
        PPOConfig()
        # Use new API stack.
        .experimental(_enable_new_api_stack=True)
        .environment(StatelessCartPole)
        .rl_module(rl_module_spec=rlm_spec)
        # And new EnvRunner.
        .rollouts(
            env_runner_cls=SingleAgentEnvRunner,
            env_to_module_connector=_env_to_module,
        )
        .resources(
            num_learner_workers=0,
        )
        .training(
            learner_connector=_learner_connector,
            num_sgd_iter=5,
            vf_loss_coeff=0.0001,
            train_batch_size=512,
            model={
                "use_lstm": True,
                "lstm_cell_size": 32,
                "fcnet_weights_initializer": torch.nn.init.xavier_uniform_,
                "fcnet_bias_initializer": (
                    functools.partial(torch.nn.init.constant_, val=0.0)
                ),
                "vf_share_layers": True,
                "uses_new_env_runners": True,
            },
        )
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=1,
            checkpoint_config=air.CheckpointConfig(checkpoint_at_end=False),
        ),
        tune_config=tune.TuneConfig(num_samples=1),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()

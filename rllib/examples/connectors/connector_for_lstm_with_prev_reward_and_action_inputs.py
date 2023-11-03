import argparse
import os

import numpy as np

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_pipeline_v2 import (
    EnvToModulePipeline,
    ModuleToEnvPipeline,
)
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.test_utils import check_learning_achieved


parser = argparse.ArgumentParser()
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument("--use-prev-action", action="store_true")
parser.add_argument("--use-prev-reward", action="store_true")
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)


class PrevRewardPrevActionConnector(ConnectorV2):
    def __init__(self, *, ctx, prev_r=False, prev_a=False):
        super().__init__(ctx=ctx)
        self.prev_r = prev_r
        self.prev_a = prev_a
        self.action_space = self.ctx.env.single_action_space

    # Only need to override the __call__ method to implement the extraction of
    # state, reward, and action data from the ongoing episodes.
    def __call__(self, input_, episodes, ctx, **kwargs):
        # This is a data-in-data-out connector, so we expect `input_` to be a dict
        # with: key=column name, e.g. "obs" and value=[data to be processed by RLModule].
        # We will just extract the most recent rewards and/or most recent actions from
        # all episodes and store them inside the `input_` data dict.

        # Make sure our expectations are correct.
        assert isinstance(input_, dict)
        assert SampleBatch.PREV_REWARDS not in input_
        assert SampleBatch.PREV_ACTIONS not in input_

        # Extract prev rewards.
        prev_r = []
        prev_a = []
        for episode in episodes:
            if self.prev_r:
                prev_r.append(episode.rewards[-1] if len(episode) > 0 else 0.0)
            if self.prev_a:
                prev_a.append(episode.actions[-1] if len(episode) > 0 else self.action_space.sample())

        if self.prev_r:
            input_[SampleBatch.PREV_REWARDS] = np.array(prev_r)
        if self.prev_a:
            # Support nested action structures.
            input_[SampleBatch.PREV_ACTIONS] = batch(prev_a)
        return input_


if __name__ == "__main__":
    import ray
    from ray import air, tune

    args = parser.parse_args()

    ray.init(local_mode=True)#TODO

    # Define our custom connector pipelines.
    def connector_creator(env, rl_module):
        # Create the connector context to use.
        ctx = ConnectorContextV2(rl_module=rl_module, env=env)
        # Create the env-to-module connector (pipeline).
        env_to_module = EnvToModulePipeline(
            ctx=ctx,
            connectors=[
                PrevRewardPrevActionConnector(
                    ctx=ctx,
                    prev_r=args.use_prev_reward,
                    prev_a=args.use_prev_action,
                ),
            ],
        )
        # Create the (empty) module-to-env connector (pipeline).
        module_to_env = ModuleToEnvPipeline(ctx=ctx)

        return env_to_module, module_to_env, ctx

    config = (
        PPOConfig()
        # Use new API stack.
        .experimental(_enable_new_api_stack=True)
        .framework(args.framework)
        .environment(StatelessCartPole)
        # And new EnvRunner.
        .rollouts(
            env_runner_cls=SingleAgentEnvRunner,
            enable_connectors=True,
            connector_creator=connector_creator,
        )
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        .training(
            num_sgd_iter=5,
            vf_loss_coeff=0.0001,
            train_batch_size=512,
            model={
                "use_lstm": True,
                "lstm_cell_size": 32,
                "vf_share_layers": True,
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
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
        ),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()

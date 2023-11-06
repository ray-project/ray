import argparse
import os

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_pipeline_v2 import (
    ConnectorPipelineV2,
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
    def __init__(self, *, ctx, as_learner_connector=False):
        super().__init__(ctx=ctx)
        self.action_space = ctx.rl_module.config.action_space
        self.as_learner_connector = as_learner_connector

    # Only need to override the __call__ method to implement the extraction of
    # reward and action data from the ongoing episodes.
    def __call__(self, *, input_, episodes, ctx, **kwargs):
        # This is a data-in-data-out connector, so we expect `input_` to be a dict
        # with: key=column name, e.g. "obs" and value=[data to be processed by RLModule].
        # We will just extract the most recent rewards and/or most recent actions from
        # all episodes and store them inside the `input_` data dict.

        # 0th reward == 0.0.
        r0 = 0.0
        # Set 0th action (prior to first action taken in episode) to all 0s.
        a0 = tree.map_structure(
            lambda s: np.zeros_like(s),
            (
                self.action_space.sample()
            ),
        )

        prev_a = []
        prev_r = []
        for episode in episodes:
            if self.as_learner_connector:
                prev_r.extend([r0] + list(episode.rewards[:-1]))
                prev_a.extend([a0] + list(episode.actions[:-1]))
            else:
                prev_a.append(episode.actions[-1] if len(episode) else a0)
                prev_r.append(episode.rewards[-1] if len(episode) else r0)

        input_[SampleBatch.PREV_ACTIONS] = batch(prev_a)
        input_[SampleBatch.PREV_REWARDS] = np.array(prev_r)
        return input_


if __name__ == "__main__":
    import ray
    from ray import air, tune

    args = parser.parse_args()

    ray.init(local_mode=True)#TODO

    # Define our custom connector pipelines.
    def make_sampling_connectors(env, rl_module):
        # Create the connector context to use.
        ctx = ConnectorContextV2(rl_module=rl_module, env=env)
        # Create the env-to-module connector.
        env_to_module = EnvToModulePipeline(
            ctx=ctx,
            connectors=[PrevRewardPrevActionConnector(ctx=ctx)]
        )
        # Leave module-to-env undefined as we don't need any special behavior
        # here.
        # TODO (sven): Allow returning None here. Also allow returning non-pipeline
        #  individual connector. RLlib should always create pipeline automatically.
        module_to_env = ModuleToEnvPipeline(ctx=ctx)

        return env_to_module, module_to_env, ctx

    def make_learner_connector(rl_module):
        # Create the connector context to use.
        ctx = ConnectorContextV2(rl_module=rl_module)
        # Create the learner connector.
        learner_connector = ConnectorPipelineV2(
            ctx=ctx,
            connectors=[
                PrevRewardPrevActionConnector(
                    ctx=ctx,
                    as_learner_connector=True,
                ),
            ],
        )
        return learner_connector, ctx


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
            sampling_connectors=make_sampling_connectors,
        )
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        .training(
            learner_connector=make_learner_connector,
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

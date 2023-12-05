import argparse
import os

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.env_to_module.prev_action_prev_reward import (
    PrevRewardPrevActionEnvToModule
)
from ray.rllib.connectors.learner.prev_action_prev_reward import (
    PrevRewardPrevActionLearner
)
from ray.rllib.connectors.connector_pipeline_v2 import (
    ConnectorPipelineV2,
    EnvToModulePipeline,
    ModuleToEnvPipeline,
)
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
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
    "--stop-iters", type=int, default=2000, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=1000000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)


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
            connectors=[PrevRewardPrevActionEnvToModule(ctx=ctx)]
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
                PrevRewardPrevActionLearner(
                    ctx=ctx,
                    as_learner_connector=True,
                ),
            ],
        )
        return learner_connector, ctx


    from ray.rllib.algorithms.dreamerv3.utils.debugging import CartPoleDebug
    config = (
        PPOConfig()
        # Use new API stack.
        .experimental(_enable_new_api_stack=True)
        .framework(args.framework)
        .environment("CartPole-v1")#StatelessCartPole)
        # And new EnvRunner.
        .rollouts(
            env_runner_cls=SingleAgentEnvRunner,
            sampling_connectors=make_sampling_connectors,
        )
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        .training(
            learner_connector=make_learner_connector,
            num_sgd_iter=5,
            vf_loss_coeff=0.0001,
            train_batch_size=512,
            #model={
            #    "use_lstm": True,
            #    "lstm_cell_size": 32,
            #    "vf_share_layers": True,
            #},
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
        run_config=air.RunConfig(stop=stop),
        #tune_config=tune.TuneConfig(num_samples=10),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()

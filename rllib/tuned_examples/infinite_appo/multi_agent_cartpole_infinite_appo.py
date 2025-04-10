from ray.rllib.algorithms.infinite_appo.infinite_appo import InfiniteAPPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(default_timesteps=2000000)
parser.add_argument(
    "--num-batch-dispatchers",
    type=int,
    default=1,
    help="Number of batch dispatch actors (layer in between "
    "AggregatorActor and Learner).",
)
parser.add_argument(
    "--num-env-runner-state-aggregators",
    type=int,
    default=1,
    help="Number of EnvRunner state aggregator actors.",
)
parser.add_argument(
    "--num-weights-server-actors",
    type=int,
    default=1,
    help="Number of weights server actors.",
)
parser.add_argument(
    "--sync-freq",
    type=int,
    default=10,
    help="Synchronization frequency between the different actor types of the pipeline.",
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_agents=2,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env("env", lambda cfg: MultiAgentCartPole(config=cfg))


config = (
    InfiniteAPPOConfig()
    .framework(torch_skip_nan_gradients=True)
    .environment(
        "env",
        env_config={"num_agents": args.num_agents},
    )
    .learners(
        num_aggregator_actors_per_learner=2,
    )
    .training(
        num_weights_server_actors=args.num_weights_server_actors,
        num_batch_dispatchers=args.num_batch_dispatchers,
        num_env_runner_state_aggregators=args.num_env_runner_state_aggregators,
        pipeline_sync_freq=args.sync_freq,

        vf_loss_coeff=0.005,
        entropy_coeff=0.0,
        broadcast_interval=1,
    )
    .rl_module(
        model_config=DefaultModelConfig(vf_share_layers=True),
    )
    .multi_agent(
        policy_mapping_fn=(lambda agent_id, episode, **kwargs: f"p{agent_id}"),
        policies={f"p{i}" for i in range(args.num_agents)},
    )
)

stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 350.0 * args.num_agents,
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": args.stop_timesteps,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)

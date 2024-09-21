from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import MeanStdFilter
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(default_timesteps=500000)
parser.set_defaults(
    enable_new_api_stack=True,
    num_agents=2,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env("multi_agent_pendulum", lambda cfg: MultiAgentPendulum(config=cfg))

config = (
    PPOConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("multi_agent_pendulum", env_config={"num_agents": args.num_agents})
    .env_runners(
        env_to_module_connector=lambda env: MeanStdFilter(multi_agent=True),
    )
    .training(
        train_batch_size_per_learner=1024,
        minibatch_size=128,
        lr=0.0002 * (args.num_gpus or 1) ** 0.5,
        gamma=0.95,
        lambda_=0.5,
    )
    .rl_module(
        model_config_dict={
            "fcnet_activation": "relu",
            "uses_new_env_runners": True,
        },
    )
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={f"p{i}" for i in range(args.num_agents)},
    )
)

stop = {
    NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
    # Divide by num_agents to get actual return per agent.
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -300.0 * (args.num_agents or 1),
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)

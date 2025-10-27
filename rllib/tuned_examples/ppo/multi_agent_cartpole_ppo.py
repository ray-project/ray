from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args()
parser.set_defaults(
    num_agents=2,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env("multi_agent_cartpole", lambda cfg: MultiAgentCartPole(config=cfg))

config = (
    PPOConfig()
    .environment("multi_agent_cartpole", env_config={"num_agents": args.num_agents})
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[32],
            fcnet_activation="linear",
            vf_share_layers=True,
        ),
    )
    .env_runners(
        num_envs_per_env_runner=2,
    )
    .training(
        lr=0.0003,
        num_epochs=6,
        vf_loss_coeff=0.01,
    )
    .multi_agent(
        policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
        policies={f"p{i}" for i in range(args.num_agents)},
    )
)

stop = {
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 400000,
    # Divide by num_agents to get actual return per agent.
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 450.0 * (args.num_agents or 1),
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)

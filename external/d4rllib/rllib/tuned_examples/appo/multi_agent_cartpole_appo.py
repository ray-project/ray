from ray.rllib.algorithms.appo import APPOConfig
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
parser.set_defaults(
    num_agents=2,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env("env", lambda cfg: MultiAgentCartPole(config=cfg))


config = (
    APPOConfig()
    .environment("env", env_config={"num_agents": args.num_agents})
    .training(
        vf_loss_coeff=0.005,
        entropy_coeff=0.0,
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

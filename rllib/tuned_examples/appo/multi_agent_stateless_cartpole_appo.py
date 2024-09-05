from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.connectors.env_to_module import MeanStdFilter
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentStatelessCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(default_timesteps=2000000)
parser.set_defaults(
    enable_new_api_stack=True,
    num_agents=2,
    num_env_runners=3,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

register_env("env", lambda cfg: MultiAgentStatelessCartPole(config=cfg))


config = (
    APPOConfig()
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("env", env_config={"num_agents": args.num_agents})
    .env_runners(
        env_to_module_connector=lambda env: MeanStdFilter(multi_agent=True),
    )
    .training(
        train_batch_size_per_learner=600,
        lr=0.0005 * ((args.num_gpus or 1) ** 0.5),
        num_epochs=1,
        vf_loss_coeff=0.05,
        grad_clip=20.0,
    )
    .rl_module(
        model_config_dict={
            "use_lstm": True,
            "uses_new_env_runners": True,
            "max_seq_len": 20,
        },
    )
    .multi_agent(
        policy_mapping_fn=(lambda agent_id, episode, **kwargs: f"p{agent_id}"),
        policies={f"p{i}" for i in range(args.num_agents)},
    )
)

stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 200.0 * args.num_agents,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
}

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)

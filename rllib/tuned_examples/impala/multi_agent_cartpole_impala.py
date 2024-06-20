from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args()
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

register_env("env", lambda cfg: MultiAgentCartPole(config=cfg))


config = (
    ImpalaConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("env", env_config={"num_agents": 2})
    .env_runners(
        env_to_module_connector=lambda env: MeanStdFilter(multi_agent=True),
    )
    .training(
        train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
        lr=0.0005,
        vf_loss_coeff=0.1,
    )
    .rl_module(
        model_config_dict={
            "vf_share_layers": True,
            "uses_new_env_runners": True,
        },
    )
    .multi_agent(
        policies=["p0", "p1"],
        policy_mapping_fn=(lambda agent_id, episode, **kwargs: f"p{agent_id}"),
    )
)

stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 800.0,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 400000,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)

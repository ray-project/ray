from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args()
args = parser.parse_args()


config = (
    ImpalaConfig()
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("CartPole-v1")
    .env_runners(
        num_env_runners=2,
        #env_to_module_connector=lambda env: MeanStdFilter(),
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
)

stop = {
    "env_runner_results/episode_return_mean": 450.0,
    "num_env_steps_sampled_lifetime": 2000000,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)

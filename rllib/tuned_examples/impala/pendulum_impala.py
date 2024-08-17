from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args()
args = parser.parse_args()

config = (
    IMPALAConfig()
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .env_runners(num_envs_per_env_runner=5)
    .environment("Pendulum-v1")
    .training(
        train_batch_size_per_learner=256,
        grad_clip=40.0,
        grad_clip_by="global_norm",
        lr=0.0003 * ((args.num_gpus or 1) ** 0.5),
        vf_loss_coeff=0.05,
        entropy_coeff=[[0, 0.1], [2000000, 0.0]],
    )
    .rl_module(
        model_config_dict={
            "vf_share_layers": True,
            "fcnet_hiddens": [512, 512],
            "uses_new_env_runners": True,
        },
    )
)

stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -200.0,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 5000000,
}

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)

import gymnasium as gym

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=9000.0,
    default_timesteps=100000000,
)
parser.set_defaults(
    enable_new_api_stack=True,
    env="HalfCheetah-v4",
)
args = parser.parse_args()


# @OldAPIStack
# This can reach 9k reward in 2 hours on a Titan XP GPU
# with 16 workers and 8 envs per worker.
#        num_env_runners: 16
#        num_gpus=1

config = (
    APPOConfig()
    .env_runners(
        num_envs_per_env_runner=32,
        rollout_fragment_length=512,
        max_requests_in_flight_per_env_runner=1,
    )
    .training(
        train_batch_size_per_learner=4096,
        target_network_update_freq=2 * 16 * 20 * 4096,
        clip_param=0.2,
        num_gpu_loader_threads=1,
        grad_clip=0.5,
        grad_clip_by="value",
        lr=[
            [0, 0.0005],
            [150000000, 0.000001],
        ],
        vf_loss_coeff=0.5,
        lambda_=0.95,
        entropy_coeff=0.01,
        broadcast_interval=1,
        use_kl_loss=True,
        kl_coeff=1.0,
        kl_target=0.04,
        # learner_queue_size=1,
    )
    #.framework(
    #    torch_compile_learner=True,
    #    torch_compile_learner_dynamo_backend="inductor",
    #    torch_compile_learner_what_to_compile="complete_update",
    #)
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)

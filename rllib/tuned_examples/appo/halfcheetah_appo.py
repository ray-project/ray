from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=9000.0,
    default_timesteps=100000000,
)
parser.set_defaults(
    env="HalfCheetah-v4",
)
args = parser.parse_args()


config = (
    APPOConfig().env_runners(
        num_envs_per_env_runner=32,  # Note: Old stack yaml uses 16.
        rollout_fragment_length=512,  # Note: [1] uses 1024.
    )
    # Train on 1 (local learner) GPU.
    .learners(num_learners=0, num_gpus_per_learner=1)
    # TODO: The following hyperparameters have been taken from the paper. Some more
    #  tuning might be necessary to speed up learning further, but these settings here
    #  already show good learning behavior.
    #  The old API stack's yaml file had this in its comment:
    #  ```
    #  This can reach 9k reward in 2 hours on a Titan XP GPU with 16 workers and 8
    #  envs per worker.
    #  ```, but we have not confirmed this in some time.
    .training(
        train_batch_size_per_learner=4096,  # Note: [1] uses 32768.
        circular_buffer_num_batches=16,  # matches [1]
        circular_buffer_iterations_per_batch=20,  # Note: [1] uses 32 for HalfCheetah.
        target_network_update_freq=2,
        target_worker_clipping=2.0,  # matches [1]
        clip_param=0.4,  # matches [1]
        num_gpu_loader_threads=1,
        # Note: The paper does NOT specify, whether the 0.5 is by-value or
        # by-global-norm.
        grad_clip=0.5,
        grad_clip_by="value",
        lr=0.0005,  # Note: [1] uses 3e-4.
        vf_loss_coeff=0.5,  # matches [1]
        gamma=0.995,  # matches [1]
        lambda_=0.995,  # matches [1]
        entropy_coeff=0.0,  # matches [1]
        use_kl_loss=True,  # matches [1]
        kl_coeff=1.0,  # matches [1]
        kl_target=0.04,  # matches [1]
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)

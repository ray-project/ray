"""Example showing how to run APPO on continuous control MuJoCo environments.

This example demonstrates APPO (Asynchronous Proximal Policy Optimization) on
the HalfCheetah-v4 environment from MuJoCo. APPO's circular replay buffer and
asynchronous training make it well-suited for continuous control tasks that
benefit from increased sample efficiency.

The hyperparameters used here are based on those reported in the APPO (IMPACT) paper [1].
Most notably, this configuration uses KL divergence loss (`use_kl_loss=True`) with
adaptive KL coefficient (`kl_coeff=1.0`, `kl_target=0.04`) for improved policy
stability in continuous action spaces.

This example:
    - uses 32 parallel environments per env runner for high-throughput sampling
    - configures the circular buffer with 16 batches and 20 iterations per batch,
    allowing substantial reuse of collected experiences (these are the paper's
    recommended settings for continuous action tasks)
    - applies gradient clipping by value (0.5) rather than by global norm
    - uses a high discount factor (gamma=0.995) and GAE lambda (0.995) for better
    long-horizon credit assignment
    - trains on 1 local GPU learner by default (can be scaled up)

[1] Luo et al., "IMPACT: Importance Weighted Asynchronous Architectures with
    Clipped Target Networks", 2020. https://arxiv.org/pdf/1912.00167

How to run this script
----------------------
`python [script file name].py [options]`

To run with default settings on HalfCheetah:
`python [script file name].py`

To run on a different MuJoCo environment:
`python [script file name].py --env=Hopper-v4`

To scale up training with more env runners:
`python [script file name].py --num-env-runners=16`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of 9000.0 within
approximately 2 million timesteps. The learning curve may show some initial
instability before stabilizing as the KL coefficient adapts. Training time
depends on hardware; the old API stack reported reaching 9k reward in ~2 hours
on a Titan XP GPU with 16 workers and 8 envs per worker (though this has not
been recently confirmed with the new API stack).
"""
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=8000.0,
    default_timesteps=2_000_000,
)
parser.set_defaults(
    env="Humanoid-v4",
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

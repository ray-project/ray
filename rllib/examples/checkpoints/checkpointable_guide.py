"""
Guide on how to use RLlib's Checkpointable API for saving and restoring RLlib components.

About:
    This guide will cover common use cases of the RLlib's Checkpointable API.
    Checkpointable API allows users to save and restore various RLlib components and continue training models from a previous states.
    Users can leverage local file system as well as cloud storage solutions (like AWS S3, Google Cloud GCS)

Key RLlib Checkpointable components are:
        - Algorithm
        - LearnerGroup
        - Learner
        - RLModule

RLlib checkpoints have the following directory structure:
    algorithm
        learner_group
            learner
                rl_module
                   default_policy
                       module_state (state_dict)
        env_runner
        eval_env_runner
This structure allows users to restore any of these components individually if needed.

Guide setup:
    For the single agent RL use cases, use PPO algorithm on the CartPole-v1 environment, specifically:
        - Algorithm: PPO
        - Environment: CartPole-v1
        - RL Module: DefaultPPOTorchRLModule

    Multi-agent use cases:
        - Algorithm: PPO
        - Environment: TicTacToe (multi-agent with 5 agents)
        - RL Module: MultiRLModule with DefaultPPOTorchRLModule per agent

Use cases:
    - Saving and restoring the entire algorithm state.
    - Saving and restoring only the Learner state.
    - Saving and restoring only the RLModule state.
    - Restoring all RLModules from a checkpoint (multi-agent).
    - Restoring only selected RLModules from a checkpoint (multi-agent).
    - Restoring selected RLModules from different checkpoints (multi-agent).
"""
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)

NUM_ITERS = 2


def build_base_config(local_only=False):
    return (
        PPOConfig()
        .environment(
            env="CartPole-v1",
        )
        .env_runners(
            num_env_runners=0 if local_only else 1,
            num_envs_per_env_runner=10,
        )
        .evaluation(
            evaluation_num_env_runners=0 if local_only else 1,
            evaluation_interval=1,
            evaluation_duration_unit="episodes",
            evaluation_duration=100,
            evaluation_parallel_to_training=False,
            evaluation_force_reset_envs_before_iteration=True,
        )
        .learners(
            num_learners=0 if local_only else 1,
            num_gpus_per_learner=0,
        )
        .training(
            lr=0.0003,
            vf_loss_coeff=0.01,
            train_batch_size_per_learner=128,
            num_epochs=1,
        )
        .rl_module(
            model_config=DefaultModelConfig(
                fcnet_hiddens=[32],
                fcnet_activation="linear",
                vf_share_layers=True,
            ),
        )
    )


if __name__ == "__main__":
    # Build the algorithm
    base_config = build_base_config(local_only=True)
    base_ppo_algo = base_config.build_algo()

    # train for NUM_ITERS iterations
    for i in range(NUM_ITERS):
        base_ppo_algo.train()

    # evaluate
    eval_results = base_ppo_algo.evaluate()
    mean_return = eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]

    ###############################################
    # Save and restore the entire algorithm state #
    ###############################################

    # Save a checkpoint
    path = f"/tmp/ray/rllib_results/iter_{str(NUM_ITERS).zfill(3)}"
    checkpoint_path = base_ppo_algo.save_to_path(path=path)

    # Release resources claimed by the base_ppo_algo
    base_ppo_algo.cleanup()

    # Create a new algorithm instance (reusing the same base_config),
    # and restore its state from the checkpoint.
    new_base_ppo_algo = base_config.build_algo()
    new_base_ppo_algo.restore_from_path(path=checkpoint_path)

    # check if the algorithm is restored correctly
    base_num_env_steps_samples_lifetime = base_ppo_algo.metrics.peek(
        key=(ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
    )
    new_num_env_steps_samples_lifetime = new_base_ppo_algo.metrics.peek(
        key=(ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
    )
    base_iter = base_ppo_algo.iteration
    new_iter = new_base_ppo_algo.iteration

    assert base_num_env_steps_samples_lifetime == new_num_env_steps_samples_lifetime
    assert base_iter == new_iter

    # Continue training the restored algorithm for NUM_ITERS iterations
    for i in range(NUM_ITERS):
        new_base_ppo_algo.train()

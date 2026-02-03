# flake8: noqa
# fmt: off

# __sphinx_doc_offline_api_1__begin__
from ray import tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EVALUATION_RESULTS,
    EPISODE_RETURN_MEAN,
)

ppo_base_config = (
    PPOConfig()
    .environment("CartPole-v1")
    .training(
        lr=0.0003,
        num_epochs=6, # Run 6 SGD minibatch iterations on a batch.
        vf_loss_coeff=0.01,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[32],
            fcnet_activation="linear",
            vf_share_layers=True,
        ),
    )
)

# Path where we will store data for later offline training.
data_path = "/tmp/docs_rllib_offline_recording"

config = (
    ppo_base_config.copy()
    .env_runners(
        batch_mode="complete_episodes",
    )
    .evaluation(
        evaluation_num_env_runners=5,
        evaluation_duration=50,
        evaluation_duration_unit="episodes",
    )
    .offline_data(
        output=data_path,
        output_write_episodes=True,
        output_max_rows_per_file=25,
    )
)

# We'll use this metric to stop the training
metric = f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"

# Define the Tuner.
tuner = tune.Tuner(
    "PPO",
    param_space=config,
    run_config=tune.RunConfig(
        stop={
            metric: 450.0,
        },
        name="docs_rllib_offline_pretrain_ppo",
    ),
)
results = tuner.fit()

best_checkpoint = (
    results
    .get_best_result(
        metric=metric,
        mode="max"
    )
    .checkpoint.path
)
# __sphinx_doc_offline_api_1__end__


# __sphinx_doc_offline_api_2__begin__
from ray.rllib.core import COMPONENT_RL_MODULE
# Ray data will write a parquet
data_path = "/tmp/docs_rllib_offline_recording"

algo = config.build()
# Load only the `RLModule` component here.
algo.restore_from_path(
    best_checkpoint,
    component=COMPONENT_RL_MODULE,
)

# Record the data.
for i in range(10):
    eval_results = algo.evaluate()
    print(eval_results)

# Stop the algorithm. Note, this is important for when
# defining `output_max_rows_per_file`. Otherwise,
# remaining episodes in the `EnvRunner`s buffer isn't written to disk.
algo.stop()
# __sphinx_doc_offline_api_2__end__


# __sphinx_doc_offline_api_3__begin__
from ray.rllib.algorithms.bc import BCConfig
# Setup the config for behavior cloning.
config = (
    BCConfig()
    .environment(
        env="CartPole-v1",
    )
    .training(
        # This has to be defined in the new offline RL API.
        train_batch_size_per_learner=1024,
    )
    .offline_data(
        input_=[data_path],
        input_read_episodes=True,
        # Create exactly 2 `DataWorkers` that transform
        # the data on-the-fly. Give each of them a single
        # CPU.
        map_batches_kwargs={
            "concurrency": 2,
            "num_cpus": 1,
        },
        # When iterating over the data, prefetch two batches
        # to improve the data pipeline. Don't shuffle the
        # buffer (the data is too small).
        iter_batches_kwargs={
            "prefetch_batches": 2,
            "local_shuffle_buffer_size": None,
        },
        # You must set this for single-learner setups.
        dataset_num_iters_per_learner=1,
    )
    .evaluation(
        # Run evaluation to see how well the learned policy
        # performs. Run every 3rd training iteration an evaluation.
        evaluation_interval=3,
        # Use a single `EnvRunner` for evaluation.
        evaluation_num_env_runners=1,
        # In each evaluation rollout, collect 5 episodes of data.
        evaluation_duration=5,
        # Evaluate the policy parallel to training.
        evaluation_parallel_to_training=True,
    )
)

# Set the stopping metric to be the evaluation episode return mean.
metric = f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"

# Configure Ray Tune.
tuner = tune.Tuner(
    "BC",
    param_space=config,
    run_config=tune.RunConfig(
        name="docs_rllib_offline_bc",
        # Stop behavior cloning when we reach 450 in return.
        stop={metric: 450.0},
        checkpoint_config=tune.CheckpointConfig(
            # Only checkpoint at the end to be faster.
            checkpoint_frequency=0,
            checkpoint_at_end=True,
        ),
        verbose=2,
    )
)
# Run the experiment.
analysis = tuner.fit()
# __sphinx_doc_offline_api_3__end__


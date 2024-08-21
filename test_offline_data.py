"""Script tests the performance of `map_batches` and all componets.

Using `map_batches` is very slow at this moment. We use `map_batches`
as preprocessing channel for our (remote/local) learner(s) to digest
data before computing loss. The main idea is to make prefetching
possible and preprocess data batches while others are used already
in learner(s).

This script tests the single steps performed before, in, and after
`map_batches` to test where the bottleneck lies.

Use `NUM_TEST_ITERATIONS`  to modify the number of iterations for
performance timing.
"""
import time
from pathlib import Path

import ray
from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.offline.offline_data import OfflineData, OfflinePreLearner
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch

# The number of iterations over which performance time will be averaged.
NUM_TEST_ITERATIONS = 10

# For data we use our test data for `CartPole-v1` (~16k rows).
data_path = "tests/data/cartpole/cartpole-v1_large"
base_path = Path(__file__).parents[0].joinpath("rllib")
data_path = "local://" + base_path.joinpath(data_path).as_posix()

# Setup the algorithm config. We need this config as c'tor arguments
# for the `OfflinePreLearner` (the preprocessor for batches in `map_batches`).
config = (
    BCConfig()
    .environment(env="CartPole-v1")
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .learners(
        num_learners=0,
    )
    .evaluation(
        evaluation_interval=3,
        evaluation_num_env_runners=1,
        evaluation_duration=5,
        evaluation_parallel_to_training=True,
    )
    # Note, the `input_` argument is the major argument for the
    # new offline API.
    .offline_data(
        input_=[data_path],
        dataset_num_iters_per_learner=1,
        map_batches_kwargs={},
        iter_batches_kwargs={},
    )
    .training(
        lr=0.0008,
        train_batch_size_per_learner=2000,
    )
)

# --------------------------------------------------
# 1. Test `iter_batches` without any of `RLlib`s code.
# Expected value: ~0.04s.

# Load the dataset. Note, the RLlib wrapped data logic uses
# `override_num_blocks=2` by default, so we use it here, too.
# To modify arguments to `read_parquet` in `RLlib` use
# `BCConfig.offline_data.read_input_method_kwargs`.
data = ray.data.read_parquet(data_path, override_num_blocks=2)

i = 0
start = time.perf_counter()
# Note, we use here again `RLlib`s, default for `iter_batches`. To
# change the arguments use `BCConfig.offline_data.iter_batches_kwargs`.
for batch in data.iter_batches(batch_size=2000, prefetch_batches=2):
    i += 1
    batch = batch
stop = time.perf_counter()
del data
print(f" Time for sampling directly: {(stop - start) / (i + 1)}")


# --------------------------------------------------
# 2. Test `iter_batches` with `RLlib`s wrapping.
# Expected value: ~0.04s.

# Setup `OfflineData` with our config. Note, this loads the dataset
# as above into the attribute class `data`.
offline_data = OfflineData(config)

# Iterate over the dataset stored in the `OfflineData` instance.
start = time.perf_counter()
i = 0
for batch in offline_data.data.iter_batches(batch_size=2000, prefetch_batches=2):
    batch = batch
    i += 1
stop = time.perf_counter()
print(f"Time to iterate a batch: {(stop - start)/i} secs")
print(f"Batch size: {batch['obs'].shape}")

# --------------------------------------------------
# 3. Test the first part of the `OfflinePreLearner`, namely mapping to
#   RLlib's `SingleAgentEpisode`s. The `OfflinePreLearner` is used in
#   `map_batches`.
# Expected value: ~0.007s.
start = time.perf_counter()
for i in range(NUM_TEST_ITERATIONS):
    episodes = OfflinePreLearner._map_to_episodes(False, batch)["episodes"]
stop = time.perf_counter()
print(f"Time converting to episodes: {(stop - start) / NUM_TEST_ITERATIONS} secs")


# --------------------------------------------------
# 4. Test the second and third part of the `OfflinePreLearner`, namely,
#   running the learner connectors and converting to `RLlib`'s
#   `MultiAgentBatch`.
# Expected value: ~0.005s.
algo = config.build()
learner = algo.learner_group._learner

start = time.perf_counter()
for i in range(NUM_TEST_ITERATIONS):
    batch = learner._learner_connector(
        rl_module=learner._module,
        data={},
        episodes=episodes,
        shared_data={},
    )

    batch = MultiAgentBatch(
        {
            module_id: SampleBatch(module_data)
            for module_id, module_data in batch.items()
        },
        # TODO (simon): This can be run once for the batch and the
        # metrics, but we run it twice: here and later in the learner.
        env_steps=sum(e.env_steps() for e in episodes),
    )
stop = time.perf_counter()
print(
    "Time for running the learner connectors and converting to "
    f"MA-batch: {(stop - start) / NUM_TEST_ITERATIONS} secs"
)

# --------------------------------------------------
# 5. Run the complete `map_batches` as used in `RLlib`s preprocessing.
#   Note, we use here the default arguments used in `RLlib`'s
#   `OfflineData.sample` method (here with a single local learner, i.e.
#   `num_shards < 1`). To change the default arguments to `map_batches`
#   in `RLlib` use `BCConfig.offline_data.map_batches_kwargs`.
# Expected value: 4.5s
start = time.perf_counter()
for i, batch in enumerate(
    algo.offline_data.data.map_batches(
        OfflinePreLearner,
        fn_constructor_kwargs={
            "config": config,
            "learner": learner,
        },
        concurrency=2,
        batch_size=2000,
    ).iter_batches(batch_size=2000, prefetch_batches=2)
):
    batch = batch["batch"]
stop = time.perf_counter()
print(f"Batch: {batch}")
print(f"Time to pull batches and map them: {(stop - start)/ (i + 1)} secs")

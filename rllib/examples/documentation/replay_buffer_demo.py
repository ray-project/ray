# Demonstration of RLlib's ReplayBuffer workflow

from typing import Optional
import random

from ray import air, tune
from ray.rllib.utils.replay_buffers import ReplayBuffer, StorageUnit
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.algorithms.dqn.dqn import DQNConfig


# __sphinx_doc_replay_buffer_type_specification__begin__
config = DQNConfig().training(replay_buffer_config={"type": ReplayBuffer})

another_config = DQNConfig().training(replay_buffer_config={"type": "ReplayBuffer"})


yet_another_config = DQNConfig().training(
    replay_buffer_config={"type": "ray.rllib.utils.replay_buffers.ReplayBuffer"}
)

validate_buffer_config(config)
validate_buffer_config(another_config)
validate_buffer_config(yet_another_config)

# After validation, all three configs yield the same effective config
assert (
    config.replay_buffer_config
    == another_config.replay_buffer_config
    == yet_another_config.replay_buffer_config
)

# __sphinx_doc_replay_buffer_type_specification__end__


# __sphinx_doc_replay_buffer_basic_interaction__begin__
# We choose fragments because it does not impose restrictions on our batch to be added
buffer = ReplayBuffer(capacity_items=2, storage_unit=StorageUnit.FRAGMENTS)
dummy_batch = SampleBatch({"a": [1], "b": [2]})
buffer.add(dummy_batch)
buffer.sample(2)
# Because elements can be sampled multiple times, we receive a concatenated version
# of dummy_batch `{a: [1, 1], b: [2, 2,]}`.
# __sphinx_doc_replay_buffer_basic_interaction__end__


# __sphinx_doc_replay_buffer_own_buffer__begin__
class SimpleMixInBuffer(ReplayBuffer):
    @override(ReplayBuffer)
    def sample(
        self, num_items: int, evict_sampled_more_then: int = 30, **kwargs
    ) -> Optional[SampleBatchType]:
        """At least 1/2 of samples is always newest samples.
        This is similar to mixin sampling.
        """
        newest_samples = [
            self._storage._get_internal_index(len(self._storage) - i)
            for i in range(num_items // 2)
        ]
        random_samples = [
            random.randint(0, len(self) - 1) for _ in range(num_items // 2)
        ]

        sample = self._encode_sample(newest_samples + random_samples)
        self._num_timesteps_sampled += sample.count

        return sample


config = (
    DQNConfig()
    .training(replay_buffer_config={"type": SimpleMixInBuffer})
    .environment(env="CartPole-v1")
)

tune.Tuner(
    "DQN",
    param_space=config.to_dict(),
    run_config=air.RunConfig(
        stop={"training_iteration": 1},
    ),
).fit()
# __sphinx_doc_replay_buffer_own_buffer__end__

# __sphinx_doc_replay_buffer_advanced_usage_storage_unit__begin__
# This line will make our buffer store only complete episodes found in a batch
config.training(replay_buffer_config={"storage_unit": StorageUnit.EPISODES})

less_sampled_buffer = SimpleMixInBuffer(**config.replay_buffer_config)

# Gather some random experiences
env = RandomEnv()
terminated = truncated = False
batch = SampleBatch({})
t = 0
while not terminated and not truncated:
    obs, reward, terminated, truncated, info = env.step([0, 0])
    # Note that in order for RLlib to find out about start and end of an episode,
    # "t" and "terminateds" have to properly mark an episode's trajectory
    one_step_batch = SampleBatch(
        {
            "obs": [obs],
            "t": [t],
            "reward": [reward],
            "terminateds": [terminated],
            "truncateds": [truncated],
        }
    )
    batch = concat_samples([batch, one_step_batch])
    t += 1

# __sphinx_doc_replay_buffer_advanced_usage_storage_unit__end__


# __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__begin__
config = {
    "env": "CartPole-v1",
    "replay_buffer_config": {
        "type": "MultiAgentReplayBuffer",
        "underlying_replay_buffer_config": {
            "type": SimpleMixInBuffer,
            "evict_sampled_more_then": 20  # We can specify the default call argument
            # for the sample method of the underlying buffer method here
        },
    },
}

tune.Tuner(
    "DQN",
    param_space=config.to_dict(),
    run_config=air.RunConfig(
        stop={"episode_reward_mean": 50, "training_iteration": 10}
    ),
).fit()
# __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__end__

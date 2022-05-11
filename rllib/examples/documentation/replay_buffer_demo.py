# Demonstration of RLlib's ReplayBuffer workflow

from typing import Optional
import random
import numpy as np

from ray import tune
from ray.rllib.utils.replay_buffers import ReplayBuffer, StorageUnit
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.policy.sample_batch import SampleBatch



# __sphinx_doc_replay_buffer_type_specification__begin__

config = {
    "env": "CartPole-v1",
    "replay_buffer_config": {"type": ReplayBuffer},  # Specify buffer explicitly
}

another_config = {
    "env": "CartPole-v1",
    "replay_buffer_config": {"type": "ReplayBuffer"}  # Specify buffer type name as it can be found in the buffers directory
}

yet_another_config = {
    "env": "CartPole-v1",
    "replay_buffer_config": {"type": "ray.rllib.utils.ReplayBuffer"}  # Specify buffer through explicit path
}

# These three configurations all yield the same effective config
print(validate_buffer_config(config) == validate_buffer_config(another_config) == validate_buffer_config(yet_another_config))


# __sphinx_doc_replay_buffer_type_specification__end__

# __sphinx_doc_replay_buffer_own_buffer__begin__

class LessSampledReplayBuffer(ReplayBuffer):
    @PublicAPI
    @override(ReplayBuffer)
    def sample(self, num_items: int, evict_sampled_more_then: int = 100,
               **kwargs) -> Optional[SampleBatchType]:
        """Evicts experiences that have been sampled > evict_sampled_more_then times."""
        idxes = [random.randint(0, len(self) - 1) for _ in range(num_items)]
        often_sampled_idxes = list(filter(lambda x: self._hit_count[x] >=
                                                     evict_sampled_more_then))

        for idx in often_sampled_idxes:
            del self._storage[idx]
            self._hit_count[idx] = np.append(self._hit_count[:idx], self._hit_count[
                                                                    idx +
                                                                                1:])

        sample = self._encode_sample(idxes)
        self._num_timesteps_sampled += sample.count
        return sample


config["replay_buffer_config"]["type"] = LessSampledReplayBuffer

tune.run("SimpleQ", config={"env": "CartPole=v1"}, stop={"training_iteration": 1})

# __sphinx_doc_replay_buffer_own_buffer__end__

# __sphinx_doc_replay_buffer_advanced_usage_storage_unit__begin__

config["replay_buffer_config"]["storage_unit"] = StorageUnit.EPISODES

less_sampled_buffer = LessSampledReplayBuffer(**config["replay_buffer_config"])

# Gather some random experiences
env = RandomEnv()
done = False
batch = SampleBatch({})
while not done:
    obs, reward, done, info = env.step([0, 0])
    one_step_batch = SampleBatch({"obs": obs, "reward": reward, "done": done})
    batch = SampleBatch.concat_samples(batch, one_step_batch)

less_sampled_buffer.add(batch)
for i in range(3):
    less_sampled_buffer.sample(1, evict_sampled_more_then=2)

# __sphinx_doc_replay_buffer_advanced_usage_storage_unit__end__


# __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__begin__





# __sphinx_doc_replay_buffer_advanced_usage_underlying_buffers__end__

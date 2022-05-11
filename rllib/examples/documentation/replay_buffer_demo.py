# Demonstration of RLlib's ReplayBuffer workflow

from typing import Optional
import random

import ray
from ray import tune
from ray.rllib.utils.replay_buffers import ReplayBuffer, StorageUnit
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.utils.replay_buffers.utils import validate_buffer_config



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

class MyBuffer(ReplayBuffer):
    @PublicAPI
    @override(ReplayBuffer)
    def sample(self, num_items: int, select_sampled_less_then: int = 100,
               **kwargs) -> Optional[SampleBatchType]:
        """Samples only experiences that have been sampled < select_sampled_less_then"""
        idxes = [random.randint(0, len(self) - 1) for _ in range(num_items)]
        lesser_sampled_idxes = list(filter(lambda x: self._hit_count[x] <
                                                     select_sampled_less_then))
        # We honour num_items and simply duplicate a couple of items
        while len(idxes < num_items):
            idxes.append(random.choice(lesser_sampled_idxes))
        sample = self._encode_sample(idxes)
        self._num_timesteps_sampled += sample.count
        return sample


config["replay_buffer_config"]["type"] = MyBuffer

tune.run("SimpleQ", config={"env": "CartPole=v1"}, stop={"training_iteration": 1})

# __sphinx_doc_replay_buffer_own_buffer__end__

# __sphinx_doc_replay_buffer_advanced_usage__begin__

config["replay_buffer_config"]["storage_unit"] = StorageUnit.EPISODES,

# This will yield suboptimal results
tune.run("SimpleQ", config=config, stop={"training_iteration": 1})

# __sphinx_doc_replay_buffer_advanced_usage__end__

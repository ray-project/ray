import unittest
import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.minibatch_utils import (
    MiniBatchCyclicIterator,
    ShardEpisodesIterator,
)
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

tf1, tf, tfv = try_import_tf()
tf1.enable_eager_execution()

CONFIGS = [
    {"minibatch_size": 256, "num_epochs": 30, "agent_steps": (1652, 1463)},
    {"minibatch_size": 128, "num_epochs": 10, "agent_steps": (1000, 2)},
    {"minibatch_size": 128, "num_epochs": 3, "agent_steps": (56, 56)},
    {"minibatch_size": 128, "num_epochs": 7, "agent_steps": (56, 56)},
    {"minibatch_size": 128, "num_epochs": 10, "agent_steps": (56, 56)},
    {"minibatch_size": 128, "num_epochs": 10, "agent_steps": (56, 3)},
    {"minibatch_size": 128, "num_epochs": 10, "agent_steps": (56, 4)},
    {"minibatch_size": 128, "num_epochs": 10, "agent_steps": (56, 55)},
    {"minibatch_size": 128, "num_epochs": 10, "agent_steps": (400, 400)},
    {"minibatch_size": 128, "num_epochs": 10, "agent_steps": (64, 64)},
    # W/ SEQ_LENS.
    {
        "minibatch_size": 64,
        "num_epochs": 1,
        "agent_steps": (128,),
        "seq_lens": [16, 16, 16, 16, 16, 16, 2, 2, 14, 14],
        "padding": True,
    },
]


class TestMinibatchUtils(unittest.TestCase):
    def test_minibatch_cyclic_iterator(self):

        for config in CONFIGS:
            minibatch_size = config["minibatch_size"]
            num_epochs = config["num_epochs"]
            agent_steps = config["agent_steps"]
            seq_lens = config.get("seq_lens")
            max_seq_len = None
            if seq_lens:
                max_seq_len = max(seq_lens)
            padding = config.get("padding", False)
            num_env_steps = max(agent_steps)

            for backend in ["torch", "numpy"]:
                sample_batches = {
                    f"pol{i}": SampleBatch(
                        {
                            "obs": np.arange(agent_steps[i]),
                            "seq_lens": seq_lens,
                        }
                    )
                    if not seq_lens or not padding
                    else SampleBatch(
                        {
                            "obs": np.concatenate(
                                [
                                    np.concatenate(
                                        [
                                            np.arange(s),
                                            np.zeros(shape=(max_seq_len - s,)),
                                        ]
                                    )
                                    for s in seq_lens
                                ]
                            ),
                            "seq_lens": seq_lens,
                        },
                        _zero_padded=padding,
                    )
                    for i in range(len(agent_steps))
                }
                if backend == "torch":
                    for pid, batch in sample_batches.items():
                        batch["obs"] = convert_to_torch_tensor(batch["obs"])
                        if seq_lens:
                            batch["seq_lens"] = convert_to_torch_tensor(
                                batch["seq_lens"]
                            )

                mb = MultiAgentBatch(sample_batches, num_env_steps)
                batch_iter = MiniBatchCyclicIterator(
                    mb,
                    minibatch_size=minibatch_size,
                    num_epochs=num_epochs,
                    shuffle_batch_per_epoch=False,
                )
                print(config)
                iteration_counter = 0
                for batch in batch_iter:
                    print(batch)
                    print("-" * 80)
                    print(batch["pol0"]["obs"])
                    print("*" * 80)
                    # Check that for each policy the batch size is equal to the
                    # minibatch_size.
                    for policy_batch in batch.policy_batches.values():
                        check(policy_batch.count, minibatch_size)
                    iteration_counter += 1

                # For each policy check that the last item in batch matches the expected
                # values, i.e. iteration_counter * minibatch_size % agent_steps - 1.
                total_steps = iteration_counter * minibatch_size
                for policy_idx, policy_batch in enumerate(
                    batch.policy_batches.values()
                ):
                    expected_last_item = (total_steps - 1) % agent_steps[policy_idx]
                    if seq_lens and seq_lens[-1] < max_seq_len:
                        expected_last_item = 0.0
                    check(policy_batch["obs"][-1], expected_last_item)

                # Check iteration counter (should be
                # ceil(num_gsd_iter * max(agent_steps) / minibatch_size)).
                expected_iteration_counter = np.ceil(
                    num_epochs * max(agent_steps) / minibatch_size
                )
                if not seq_lens:
                    check(iteration_counter, expected_iteration_counter)
                print(f"iteration_counter: {iteration_counter}")

    def test_shard_episodes_iterator(self):
        class DummyEpisode:
            def __init__(self, length):
                self.length = length
                # Dummy data to represent the episode content.
                self.data = [0] * length

            def __len__(self):
                return self.length

            def __getitem__(self, key):
                assert isinstance(key, slice)
                return self.slice(key)

            def slice(self, slice, len_lookback_buffer=None):
                # Create a new Episode object with the sliced length
                return DummyEpisode(len(self.data[slice]))

            def __repr__(self):
                return f"{(type(self).__name__)}({self.length})"

        # Create a list of episodes with varying lengths
        episode_lens = [10, 21, 3, 4, 35, 41, 5, 15, 44]

        episodes = [DummyEpisode(len_) for len_ in episode_lens]

        # Number of shards
        num_shards = 3
        # Create the iterator
        iterator = ShardEpisodesIterator(episodes, num_shards)
        # Iterate and collect the results
        shards = list(iterator)
        # The sharder should try to split as few times as possible. In our
        # case here, only the len=4 episode is split into 1 and 3. All other
        # episodes are kept as-is. Yet, the resulting sub-lists have all
        # either size 59 or 60.
        check([len(e) for e in shards[0]], [44, 10, 5])  # 59
        check([len(e) for e in shards[1]], [41, 15, 3])  # 59
        check([len(e) for e in shards[2]], [35, 21, 1, 3])  # 60

        # Different number of shards.
        num_shards = 4
        # Create the iterator.
        iterator = ShardEpisodesIterator(episodes, num_shards)
        # Iterate and collect the results
        shards = list(iterator)
        # The sharder should try to split as few times as possible, keeping
        # as many episodes as-is (w/o splitting).
        check([len(e) for e in shards[0]], [44])  # 44
        check([len(e) for e in shards[1]], [41, 3])  # 44
        check([len(e) for e in shards[2]], [35, 10])  # 45
        check([len(e) for e in shards[3]], [21, 15, 5, 1, 3])  # 45


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

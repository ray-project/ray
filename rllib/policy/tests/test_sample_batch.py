import numpy as np
import unittest

import ray
from ray.rllib.policy.sample_batch import SampleBatch


class TestSampleBatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_dict_properties_of_sample_batches(self):
        base_dict = {
            "a": np.array([1, 2, 3]),
            "b": np.array([[0.1, 0.2], [0.3, 0.4]]),
            "c": True,
        }
        batch = SampleBatch(base_dict, _dont_check_lens=True)
        try:
            SampleBatch(base_dict, _dont_check_lens=False)
        except AssertionError:
            pass  # expected
        keys_ = list(base_dict.keys())
        values_ = list(base_dict.values())
        items_ = list(base_dict.items())
        assert list(batch.keys()) == keys_
        assert list(batch.values()) == values_
        assert list(batch.items()) == items_

        # Add an item and check, whether it's in the "added" list.
        batch["d"] = np.array(1)
        assert batch.added_keys == {"d"}
        # Access two keys and check, whether they are in the
        # "accessed" list.
        print(batch["a"], batch["b"])
        assert batch.accessed_keys == {"a", "b"}
        # Delete a key and check, whether it's in the "deleted" list.
        del batch["c"]
        assert batch.deleted_keys == {"c"}


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

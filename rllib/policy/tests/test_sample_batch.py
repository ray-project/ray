import numpy as np
import unittest

import ray
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.compression import is_compressed
from ray.rllib.utils.test_utils import check


class TestSampleBatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_len_and_size_bytes(self):
        s1 = SampleBatch({
            "a": np.array([1, 2, 3]),
            "b": {
                "c": np.array([4, 5, 6])
            },
            "seq_lens": [1, 2],
        })
        check(len(s1), 3)
        check(s1.size_bytes(),
              s1["a"].nbytes + s1["b"]["c"].nbytes + s1["seq_lens"].nbytes)

    def test_dict_properties_of_sample_batches(self):
        base_dict = {
            "a": np.array([1, 2, 3]),
            "b": np.array([[0.1, 0.2], [0.3, 0.4]]),
            "c": True,
        }
        batch = SampleBatch(base_dict)
        keys_ = list(base_dict.keys())
        values_ = list(base_dict.values())
        items_ = list(base_dict.items())
        assert list(batch.keys()) == keys_
        assert list(batch.values()) == values_
        assert list(batch.items()) == items_

        # Add an item and check, whether it's in the "added" list.
        batch["d"] = np.array(1)
        assert batch.added_keys == {"d"}, batch.added_keys
        # Access two keys and check, whether they are in the
        # "accessed" list.
        print(batch["a"], batch["b"])
        assert batch.accessed_keys == {"a", "b"}, batch.accessed_keys
        # Delete a key and check, whether it's in the "deleted" list.
        del batch["c"]
        assert batch.deleted_keys == {"c"}, batch.deleted_keys

    def test_right_zero_padding(self):
        """Tests, whether right-zero-padding work properly."""
        s1 = SampleBatch({
            "a": np.array([1, 2, 3]),
            "b": {
                "c": np.array([4, 5, 6])
            },
            "seq_lens": [1, 2],
        })
        s1.right_zero_pad(max_seq_len=5)
        check(
            s1, {
                "a": [1, 0, 0, 0, 0, 2, 3, 0, 0, 0],
                "b": {
                    "c": [4, 0, 0, 0, 0, 5, 6, 0, 0, 0]
                },
                "seq_lens": [1, 2]
            })

    def test_concat(self):
        """Tests, SampleBatches.concat() and ...concat_samples()."""
        s1 = SampleBatch({
            "a": np.array([1, 2, 3]),
            "b": {
                "c": np.array([4, 5, 6])
            },
        })
        s2 = SampleBatch({
            "a": np.array([2, 3, 4]),
            "b": {
                "c": np.array([5, 6, 7])
            },
        })
        concatd = SampleBatch.concat_samples([s1, s2])
        check(concatd["a"], [1, 2, 3, 2, 3, 4])
        check(concatd["b"]["c"], [4, 5, 6, 5, 6, 7])
        check(next(concatd.rows()), {"a": 1, "b": {"c": 4}})

        concatd_2 = s1.concat(s2)
        check(concatd, concatd_2)

    def test_rows(self):
        s1 = SampleBatch({
            "a": np.array([[1, 1], [2, 2], [3, 3]]),
            "b": {
                "c": np.array([[4, 4], [5, 5], [6, 6]])
            },
            "seq_lens": np.array([1, 2]),
        })
        check(
            next(s1.rows()),
            {
                "a": [1, 1],
                "b": {
                    "c": [4, 4]
                },
                "seq_lens": [1]
            },
        )

    def test_compression(self):
        """Tests, whether compression and decompression work properly."""
        s1 = SampleBatch({
            "a": np.array([1, 2, 3, 2, 3, 4]),
            "b": {
                "c": np.array([4, 5, 6, 5, 6, 7])
            },
        })
        # Test, whether compressing happens in-place.
        s1.compress(columns={"a", "b"}, bulk=True)
        self.assertTrue(is_compressed(s1["a"]))
        self.assertTrue(is_compressed(s1["b"]["c"]))
        self.assertTrue(isinstance(s1["b"], dict))

        # Test, whether de-compressing happens in-place.
        s1.decompress_if_needed(columns={"a", "b"})
        check(s1["a"], [1, 2, 3, 2, 3, 4])
        check(s1["b"]["c"], [4, 5, 6, 5, 6, 7])
        it = s1.rows()
        next(it)
        check(next(it), {"a": 2, "b": {"c": 5}})


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

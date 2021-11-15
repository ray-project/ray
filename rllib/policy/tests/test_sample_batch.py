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
            SampleBatch.SEQ_LENS: [1, 2],
        })
        check(len(s1), 3)
        check(
            s1.size_bytes(), s1["a"].nbytes + s1["b"]["c"].nbytes +
            s1[SampleBatch.SEQ_LENS].nbytes)

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
            SampleBatch.SEQ_LENS: [1, 2],
        })
        s1.right_zero_pad(max_seq_len=5)
        check(
            s1, {
                "a": [1, 0, 0, 0, 0, 2, 3, 0, 0, 0],
                "b": {
                    "c": [4, 0, 0, 0, 0, 5, 6, 0, 0, 0]
                },
                SampleBatch.SEQ_LENS: [1, 2]
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
            SampleBatch.SEQ_LENS: np.array([1, 2]),
        })
        check(
            next(s1.rows()),
            {
                "a": [1, 1],
                "b": {
                    "c": [4, 4]
                },
                SampleBatch.SEQ_LENS: [1]
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

    def test_slicing(self):
        """Tests, whether slicing can be done on SampleBatches."""
        s1 = SampleBatch({
            "a": np.array([1, 2, 3, 2, 3, 4]),
            "b": {
                "c": np.array([4, 5, 6, 5, 6, 7])
            },
        })
        check(s1[:3], {
            "a": [1, 2, 3],
            "b": {
                "c": [4, 5, 6]
            },
        })
        check(s1[0:3], {
            "a": [1, 2, 3],
            "b": {
                "c": [4, 5, 6]
            },
        })
        check(s1[1:4], {
            "a": [2, 3, 2],
            "b": {
                "c": [5, 6, 5]
            },
        })
        check(s1[1:], {
            "a": [2, 3, 2, 3, 4],
            "b": {
                "c": [5, 6, 5, 6, 7]
            },
        })
        check(s1[3:4], {
            "a": [2],
            "b": {
                "c": [5]
            },
        })

        # When we change the slice, the original SampleBatch should also
        # change (shared underlying data).
        s1[:3]["a"][0] = 100
        s1[1:2]["a"][0] = 200
        check(s1["a"][0], 100)
        check(s1["a"][1], 200)

        # Seq-len batches should be auto-sliced along sequences,
        # no matter what.
        s2 = SampleBatch({
            "a": np.array([1, 2, 3, 2, 3, 4]),
            "b": {
                "c": np.array([4, 5, 6, 5, 6, 7])
            },
            SampleBatch.SEQ_LENS: [2, 3, 1],
            "state_in_0": [1.0, 3.0, 4.0],
        })
        # We would expect a=[1, 2, 3] now, but due to the sequence
        # boundary, we stop earlier.
        check(
            s2[:3], {
                "a": [1, 2],
                "b": {
                    "c": [4, 5]
                },
                SampleBatch.SEQ_LENS: [2],
                "state_in_0": [1.0],
            })
        # Split exactly at a seq-len boundary.
        check(
            s2[:5], {
                "a": [1, 2, 3, 2, 3],
                "b": {
                    "c": [4, 5, 6, 5, 6]
                },
                SampleBatch.SEQ_LENS: [2, 3],
                "state_in_0": [1.0, 3.0],
            })
        # Split above seq-len boundary.
        check(
            s2[:50], {
                "a": [1, 2, 3, 2, 3, 4],
                "b": {
                    "c": [4, 5, 6, 5, 6, 7]
                },
                SampleBatch.SEQ_LENS: [2, 3, 1],
                "state_in_0": [1.0, 3.0, 4.0],
            })
        check(
            s2[:], {
                "a": [1, 2, 3, 2, 3, 4],
                "b": {
                    "c": [4, 5, 6, 5, 6, 7]
                },
                SampleBatch.SEQ_LENS: [2, 3, 1],
                "state_in_0": [1.0, 3.0, 4.0],
            })

    def test_copy(self):
        s = SampleBatch({
            "a": np.array([1, 2, 3, 2, 3, 4]),
            "b": {
                "c": np.array([4, 5, 6, 5, 6, 7])
            },
            SampleBatch.SEQ_LENS: [2, 3, 1],
            "state_in_0": [1.0, 3.0, 4.0],
        })
        s_copy = s.copy(shallow=False)
        s_copy["a"][0] = 100
        s_copy["b"]["c"][0] = 200
        s_copy[SampleBatch.SEQ_LENS][0] = 3
        s_copy[SampleBatch.SEQ_LENS][1] = 2
        s_copy["state_in_0"][0] = 400.0
        self.assertNotEqual(s["a"][0], s_copy["a"][0])
        self.assertNotEqual(s["b"]["c"][0], s_copy["b"]["c"][0])
        self.assertNotEqual(s[SampleBatch.SEQ_LENS][0],
                            s_copy[SampleBatch.SEQ_LENS][0])
        self.assertNotEqual(s[SampleBatch.SEQ_LENS][1],
                            s_copy[SampleBatch.SEQ_LENS][1])
        self.assertNotEqual(s["state_in_0"][0], s_copy["state_in_0"][0])

        s_copy = s.copy(shallow=True)
        s_copy["a"][0] = 100
        s_copy["b"]["c"][0] = 200
        s_copy[SampleBatch.SEQ_LENS][0] = 3
        s_copy[SampleBatch.SEQ_LENS][1] = 2
        s_copy["state_in_0"][0] = 400.0
        self.assertEqual(s["a"][0], s_copy["a"][0])
        self.assertEqual(s["b"]["c"][0], s_copy["b"]["c"][0])
        self.assertEqual(s[SampleBatch.SEQ_LENS][0],
                         s_copy[SampleBatch.SEQ_LENS][0])
        self.assertEqual(s[SampleBatch.SEQ_LENS][1],
                         s_copy[SampleBatch.SEQ_LENS][1])
        self.assertEqual(s["state_in_0"][0], s_copy["state_in_0"][0])


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

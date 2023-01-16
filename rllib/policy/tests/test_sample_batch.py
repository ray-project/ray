import copy
import functools
import os
import unittest

import numpy as np
import torch
import tree

import ray
from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.policy.sample_batch import (
    SampleBatch,
    attempt_count_timesteps,
    concat_samples,
)
from ray.rllib.utils.compression import is_compressed
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.torch_utils import convert_to_torch_tensor


class TestSampleBatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_gpus=1)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_len_and_size_bytes(self):
        s1 = SampleBatch(
            {
                "a": np.array([1, 2, 3]),
                "b": {"c": np.array([4, 5, 6])},
                SampleBatch.SEQ_LENS: [1, 2],
            }
        )
        check(len(s1), 3)
        check(
            s1.size_bytes(),
            s1["a"].nbytes + s1["b"]["c"].nbytes + s1[SampleBatch.SEQ_LENS].nbytes,
        )

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
        s1 = SampleBatch(
            {
                "a": np.array([1, 2, 3]),
                "b": {"c": np.array([4, 5, 6])},
                SampleBatch.SEQ_LENS: [1, 2],
            }
        )
        s1.right_zero_pad(max_seq_len=5)
        check(
            s1,
            {
                "a": [1, 0, 0, 0, 0, 2, 3, 0, 0, 0],
                "b": {"c": [4, 0, 0, 0, 0, 5, 6, 0, 0, 0]},
                SampleBatch.SEQ_LENS: [1, 2],
            },
        )

    def test_concat(self):
        """Tests, SampleBatches.concat() and concat_samples()."""
        s1 = SampleBatch(
            {
                "a": np.array([1, 2, 3]),
                "b": {"c": np.array([4, 5, 6])},
            }
        )
        s2 = SampleBatch(
            {
                "a": np.array([2, 3, 4]),
                "b": {"c": np.array([5, 6, 7])},
            }
        )
        concatd = concat_samples([s1, s2])
        check(concatd["a"], [1, 2, 3, 2, 3, 4])
        check(concatd["b"]["c"], [4, 5, 6, 5, 6, 7])
        check(next(concatd.rows()), {"a": 1, "b": {"c": 4}})

        concatd_2 = s1.concat(s2)
        check(concatd, concatd_2)

    def test_concat_max_seq_len(self):
        """Tests, SampleBatches.concat_samples() max_seq_len."""
        s1 = SampleBatch(
            {
                "a": np.array([1, 2, 3]),
                "b": {"c": np.array([4, 5, 6])},
                SampleBatch.SEQ_LENS: [1, 2],
            }
        )
        s2 = SampleBatch(
            {
                "a": np.array([2, 3, 4]),
                "b": {"c": np.array([5, 6, 7])},
                SampleBatch.SEQ_LENS: [3],
            }
        )

        s3 = SampleBatch(
            {
                "a": np.array([2, 3, 4]),
                "b": {"c": np.array([5, 6, 7])},
            }
        )

        concatd = concat_samples([s1, s2])
        check(concatd.max_seq_len, s2.max_seq_len)

        with self.assertRaises(ValueError):
            concat_samples([s1, s2, s3])

    def test_rows(self):
        s1 = SampleBatch(
            {
                "a": np.array([[1, 1], [2, 2], [3, 3]]),
                "b": {"c": np.array([[4, 4], [5, 5], [6, 6]])},
                SampleBatch.SEQ_LENS: np.array([1, 2]),
            }
        )
        check(
            next(s1.rows()),
            {"a": [1, 1], "b": {"c": [4, 4]}, SampleBatch.SEQ_LENS: 1},
        )

    def test_compression(self):
        """Tests, whether compression and decompression work properly."""
        s1 = SampleBatch(
            {
                "a": np.array([1, 2, 3, 2, 3, 4]),
                "b": {"c": np.array([4, 5, 6, 5, 6, 7])},
            }
        )
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
        s1 = SampleBatch(
            {
                "a": np.array([1, 2, 3, 2, 3, 4]),
                "b": {"c": np.array([4, 5, 6, 5, 6, 7])},
            }
        )
        check(
            s1[:3],
            {
                "a": [1, 2, 3],
                "b": {"c": [4, 5, 6]},
            },
        )
        check(
            s1[0:3],
            {
                "a": [1, 2, 3],
                "b": {"c": [4, 5, 6]},
            },
        )
        check(
            s1[1:4],
            {
                "a": [2, 3, 2],
                "b": {"c": [5, 6, 5]},
            },
        )
        check(
            s1[1:],
            {
                "a": [2, 3, 2, 3, 4],
                "b": {"c": [5, 6, 5, 6, 7]},
            },
        )
        check(
            s1[3:4],
            {
                "a": [2],
                "b": {"c": [5]},
            },
        )

        # When we change the slice, the original SampleBatch should also
        # change (shared underlying data).
        s1[:3]["a"][0] = 100
        s1[1:2]["a"][0] = 200
        check(s1["a"][0], 100)
        check(s1["a"][1], 200)

        # Seq-len batches should be auto-sliced along sequences,
        # no matter what.
        s2 = SampleBatch(
            {
                "a": np.array([1, 2, 3, 2, 3, 4]),
                "b": {"c": np.array([4, 5, 6, 5, 6, 7])},
                SampleBatch.SEQ_LENS: [2, 3, 1],
                "state_in_0": np.array([1.0, 3.0, 4.0]),
            }
        )
        # We would expect a=[1, 2, 3] now, but due to the sequence
        # boundary, we stop earlier.
        check(
            s2[:3],
            {
                "a": [1, 2],
                "b": {"c": [4, 5]},
                SampleBatch.SEQ_LENS: [2],
                "state_in_0": [1.0],
            },
        )
        # Split exactly at a seq-len boundary.
        check(
            s2[:5],
            {
                "a": [1, 2, 3, 2, 3],
                "b": {"c": [4, 5, 6, 5, 6]},
                SampleBatch.SEQ_LENS: [2, 3],
                "state_in_0": [1.0, 3.0],
            },
        )
        # Split above seq-len boundary.
        check(
            s2[:50],
            {
                "a": [1, 2, 3, 2, 3, 4],
                "b": {"c": [4, 5, 6, 5, 6, 7]},
                SampleBatch.SEQ_LENS: [2, 3, 1],
                "state_in_0": [1.0, 3.0, 4.0],
            },
        )
        check(
            s2[:],
            {
                "a": [1, 2, 3, 2, 3, 4],
                "b": {"c": [4, 5, 6, 5, 6, 7]},
                SampleBatch.SEQ_LENS: [2, 3, 1],
                "state_in_0": [1.0, 3.0, 4.0],
            },
        )

    def test_split_by_episode(self):
        s = SampleBatch(
            {
                "a": np.array([0, 1, 2, 3, 4, 5]),
                "eps_id": np.array([0, 0, 0, 0, 1, 1]),
                "terminateds": np.array([0, 0, 0, 1, 0, 1]),
            }
        )
        true_split = [np.array([0, 1, 2, 3]), np.array([4, 5])]

        # Check that splitting by EPS_ID works correctly
        eps_split = [b["a"] for b in s.split_by_episode()]
        check(true_split, eps_split)

        # Check that splitting by EPS_ID works correctly when explicitly specified
        eps_split = [b["a"] for b in s.split_by_episode(key="eps_id")]
        check(true_split, eps_split)

        # Check that splitting by DONES works correctly when explicitly specified
        eps_split = [b["a"] for b in s.split_by_episode(key="dones")]
        check(true_split, eps_split)

        # Check that splitting by DONES works correctly
        del s["eps_id"]
        terminateds_split = [b["a"] for b in s.split_by_episode()]
        check(true_split, terminateds_split)

        # Check that splitting without the EPS_ID or DONES key raise an error
        del s["terminateds"]
        with self.assertRaises(KeyError):
            s.split_by_episode()

        # Check that splitting with DONES always False returns the whole batch
        s["terminateds"] = np.array([0, 0, 0, 0, 0, 0])
        batch_split = [b["a"] for b in s.split_by_episode()]
        check(s["a"], batch_split[0])

    def test_copy(self):
        s = SampleBatch(
            {
                "a": np.array([1, 2, 3, 2, 3, 4]),
                "b": {"c": np.array([4, 5, 6, 5, 6, 7])},
                SampleBatch.SEQ_LENS: [2, 3, 1],
                "state_in_0": np.array([1.0, 3.0, 4.0]),
            }
        )
        s_copy = s.copy(shallow=False)
        s_copy["a"][0] = 100
        s_copy["b"]["c"][0] = 200
        s_copy[SampleBatch.SEQ_LENS][0] = 3
        s_copy[SampleBatch.SEQ_LENS][1] = 2
        s_copy["state_in_0"][0] = 400.0
        self.assertNotEqual(s["a"][0], s_copy["a"][0])
        self.assertNotEqual(s["b"]["c"][0], s_copy["b"]["c"][0])
        self.assertNotEqual(s[SampleBatch.SEQ_LENS][0], s_copy[SampleBatch.SEQ_LENS][0])
        self.assertNotEqual(s[SampleBatch.SEQ_LENS][1], s_copy[SampleBatch.SEQ_LENS][1])
        self.assertNotEqual(s["state_in_0"][0], s_copy["state_in_0"][0])

        s_copy = s.copy(shallow=True)
        s_copy["a"][0] = 100
        s_copy["b"]["c"][0] = 200
        s_copy[SampleBatch.SEQ_LENS][0] = 3
        s_copy[SampleBatch.SEQ_LENS][1] = 2
        s_copy["state_in_0"][0] = 400.0
        self.assertEqual(s["a"][0], s_copy["a"][0])
        self.assertEqual(s["b"]["c"][0], s_copy["b"]["c"][0])
        self.assertEqual(s[SampleBatch.SEQ_LENS][0], s_copy[SampleBatch.SEQ_LENS][0])
        self.assertEqual(s[SampleBatch.SEQ_LENS][1], s_copy[SampleBatch.SEQ_LENS][1])
        self.assertEqual(s["state_in_0"][0], s_copy["state_in_0"][0])

    def test_shuffle_with_interceptor(self):
        """Tests, whether `shuffle()` clears the `intercepted_values` cache."""
        s = SampleBatch(
            {
                "a": np.array([1, 2, 3, 2, 3, 4, 3, 4, 5, 4, 5, 6, 5, 6, 7]),
            }
        )
        # Set a summy get-interceptor (returning all values, but plus 1).
        s.set_get_interceptor(lambda v: v + 1)
        # Make sure, interceptor works.
        check(s["a"], [2, 3, 4, 3, 4, 5, 4, 5, 6, 5, 6, 7, 6, 7, 8])
        s.shuffle()
        # Make sure, intercepted values are NOT the original ones (before the shuffle),
        # but have also been shuffled.
        check(s["a"], [2, 3, 4, 3, 4, 5, 4, 5, 6, 5, 6, 7, 6, 7, 8], false=True)

    def test_to_device(self):
        """Tests whether to_device works properly under different circumstances"""
        torch, _ = try_import_torch()

        # sample batch includes
        #   a numpy array (a)
        #   a nested stucture of dict, tuple and lists (b) of numpys and None
        #   info dict
        #   a nested structure that ends up with tensors and ints(c)
        #   a tensor with float64 values (d)
        #   a float64 tensor with possibly wrong device (depends on if cuda available)
        #   repeated value object with np.array leaves (f)

        cuda_available = int(os.environ.get("RLLIB_NUM_GPUS", "0")) > 0
        cuda_if_possible = torch.device("cuda:0" if cuda_available else "cpu")
        s = SampleBatch(
            {
                "a": np.array([1, 2]),
                "b": {"c": (np.array([4, 5]), np.array([5, 6]))},
                "c": {"d": torch.Tensor([1, 2]), "g": (torch.Tensor([3, 4]), 1)},
                "d": torch.Tensor([1.0, 2.0]).double(),
                "e": torch.Tensor([1.0, 2.0]).double().to(cuda_if_possible),
                "f": RepeatedValues(np.array([[1, 2, 0, 0]]), lengths=[2], max_len=4),
                SampleBatch.SEQ_LENS: np.array([2, 3, 1]),
                "state_in_0": np.array([1.0, 3.0, 4.0]),
                # INFO can have arbitrary elements, others need to conform in size
                SampleBatch.INFOS: np.array([{"a": 1}, {"b": [1, 2]}, {"c": None}]),
            }
        )

        # inplace operation for sample_batch
        s.to_device(cuda_if_possible, framework="torch")

        def _check_recursive_device_and_type(input_struct, target_device):
            def get_mismatched_types(v):
                if isinstance(v, torch.Tensor):
                    if v.device.type != target_device.type:
                        return (v.device, v.dtype)
                    if v.is_floating_point() and v.dtype != torch.float32:
                        return (v.device, v.dtype)

            tree_checks = {}
            for k, v in input_struct.items():
                tree_checks[k] = tree.map_structure(get_mismatched_types, v)

            self.assertTrue(
                all(v is None for v in tree.flatten((tree_checks))),
                f"the device type check dict: {tree_checks}",
            )

        # check if all tensors have the correct device and dtype
        _check_recursive_device_and_type(s, cuda_if_possible)

        # check repeated value
        check(s["f"].lengths, [2])
        check(s["f"].max_len, 4)
        check(s["f"].values, torch.from_numpy(np.asarray([[1, 2, 0, 0]])))

        # check infos
        check(s[SampleBatch.INFOS], np.array([{"a": 1}, {"b": [1, 2]}, {"c": None}]))

        # check c/g/1
        self.assertEqual(s["c"]["g"][1], torch.from_numpy(np.asarray(1)))

        with self.assertRaises(NotImplementedError):
            # should raise an error if framework is not torch
            s.to_device(cuda_if_possible, framework="tf")

    def test_count(self):
        # Tests if counts are what we would expect from different batches

        input_dicts_and_lengths = [
            (
                {
                    SampleBatch.OBS: {
                        "a": np.array([[1], [2], [3]]),
                        "b": np.array([[0], [0], [1]]),
                        "c": np.array([[4], [5], [6]]),
                    }
                },
                3,
            ),
            (
                {
                    SampleBatch.OBS: {
                        "a": np.array([[1, 2, 3]]),
                        "b": np.array([[0, 0, 1]]),
                        "c": np.array([[4, 5, 6]]),
                    }
                },
                1,
            ),
            (
                {
                    SampleBatch.INFOS: {
                        "a": np.array([[1], [2], [3]]),
                        "b": np.array([[0], [0], [1]]),
                        "c": np.array([[4], [5], [6]]),
                    }
                },
                0,  # This should have a length of zero, since we can ignore INFO
            ),
            (
                {
                    "state_in_0": {
                        "a": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                        "b": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                        "c": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                    },
                    "state_out_0": {
                        "a": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                        "b": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                        "c": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                    },
                    SampleBatch.OBS: {
                        "a": np.array([1, 2, 3]),
                        "b": np.array([0, 0, 1]),
                        "c": np.array([4, 5, 6]),
                    },
                },
                3,  # This should have a length of three - we count from OBS
            ),
            (
                {
                    "state_in_0": {
                        "a": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                        "b": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                        "c": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                    },
                    "state_out_0": {
                        "a": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                        "b": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                        "c": [[[1], [2], [3]], [[1], [2], [3]], [[1], [2], [3]]],
                    },
                },
                0,  # This should have a length of zero, we don't attempt to count
            ),
            (
                {
                    SampleBatch.OBS: {
                        "a": np.array([[1], [2], [3]]),
                        "b": np.array([[0], [0], [1]]),
                        "c": np.array([[4], [5], [6]]),
                    },
                    SampleBatch.SEQ_LENS: np.array([[1], [2], [3]]),
                },
                6,  # This should have a length of six, since we don't try to infer
                # from inputs but count by sequence lengths
            ),
            (
                {
                    SampleBatch.NEXT_OBS: {
                        "a": {"b": np.array([[1], [2], [3]])},
                        "c": np.array([[4], [5], [6]]),
                    },
                },
                3,  # Test if we properly support nesting
            ),
        ]

        for input_dict, length in input_dicts_and_lengths:
            self.assertEqual(attempt_count_timesteps(copy.deepcopy(input_dict)), length)
            s = SampleBatch(input_dict)
            self.assertEqual(s.count, length)

    def test_interceptors(self):
        # Tests whether interceptors work as intended

        some_array = np.array([1, 2, 3])
        batch = SampleBatch({SampleBatch.OBS: some_array})

        device = torch.device("cpu")

        self.assertTrue(batch[SampleBatch.OBS] is some_array)

        batch.set_get_interceptor(
            functools.partial(convert_to_torch_tensor, device=device)
        )

        self.assertTrue(
            all(convert_to_torch_tensor(some_array) == batch[SampleBatch.OBS])
        )

        # This test requires a GPU, otherwise we can't test whether we are
        # moving between devices
        if not torch.cuda.is_available():
            raise ValueError("This test can only fail if cuda is available.")

        another_array = np.array([4, 5, 6])
        another_batch = SampleBatch({SampleBatch.OBS: another_array})

        another_device = torch.device("cuda")

        self.assertTrue(another_batch[SampleBatch.OBS] is another_array)
        another_batch.set_get_interceptor(
            functools.partial(convert_to_torch_tensor, device=another_device)
        )
        check(another_batch[SampleBatch.OBS], another_array)
        self.assertFalse(another_batch[SampleBatch.OBS] is another_array)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

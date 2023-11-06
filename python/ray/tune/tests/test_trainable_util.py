import copy
from collections import OrderedDict
import pytest
import sys
import unittest
from unittest.mock import patch

from ray.tune.utils.util import wait_for_gpu
from ray.tune.utils.util import flatten_dict, unflatten_dict, unflatten_list_dict
from ray.tune.trainable.util import TrainableUtil


@pytest.mark.parametrize(
    "checkpoint_path",
    [
        "~/tmp/exp/trial/checkpoint0",
        "~/tmp/exp/trial/checkpoint0/",
        "~/tmp/exp/trial/checkpoint0/checkpoint",
        "~/tmp/exp/trial/checkpoint0/foo/bar/baz",
    ],
)
@pytest.mark.parametrize("logdir", ["~/tmp/exp/trial", "~/tmp/exp/trial/"])
def test_find_rel_checkpoint_dir(checkpoint_path, logdir):
    assert (
        TrainableUtil.find_rel_checkpoint_dir(logdir, checkpoint_path) == "checkpoint0"
    )


class FlattenDictTest(unittest.TestCase):
    def test_output_type(self):
        in_ = OrderedDict({"a": {"b": 1}, "c": {"d": 2}, "e": 3})
        out = flatten_dict(in_)
        assert type(in_) is type(out)

    def test_one_level_nested(self):
        ori_in = OrderedDict({"a": {"b": 1}, "c": {"d": 2}, "e": 3})
        in_ = copy.deepcopy(ori_in)
        result = flatten_dict(in_)
        assert in_ == ori_in
        assert result == {"a/b": 1, "c/d": 2, "e": 3}

    def test_multi_level_nested(self):
        ori_in = OrderedDict(
            {
                "a": {
                    "b": {
                        "c": {
                            "d": 1,
                        },
                    },
                },
                "b": {
                    "c": {
                        "d": 2,
                    },
                },
                "c": {
                    "d": 3,
                },
                "e": 4,
            }
        )
        in_ = copy.deepcopy(ori_in)
        result = flatten_dict(in_)
        assert in_ == ori_in
        assert result == {"a/b/c/d": 1, "b/c/d": 2, "c/d": 3, "e": 4}


class UnflattenDictTest(unittest.TestCase):
    def test_output_type(self):
        in_ = OrderedDict({"a/b": 1, "c/d": 2, "e": 3})
        out = unflatten_dict(in_)
        assert type(in_) is type(out)

    def test_one_level_nested(self):
        result = unflatten_dict({"a/b": 1, "c/d": 2, "e": 3})
        assert result == {"a": {"b": 1}, "c": {"d": 2}, "e": 3}

    def test_multi_level_nested(self):
        result = unflatten_dict({"a/b/c/d": 1, "b/c/d": 2, "c/d": 3, "e": 4})
        assert result == {
            "a": {
                "b": {
                    "c": {
                        "d": 1,
                    },
                },
            },
            "b": {
                "c": {
                    "d": 2,
                },
            },
            "c": {
                "d": 3,
            },
            "e": 4,
        }

    def test_unflatten_list_dict_output_type(self):
        in_ = OrderedDict({"a/0": 0, "a/1": 1, "c/d": 2, "e": 3})
        out = unflatten_list_dict(in_)
        assert type(out) is OrderedDict

        in_ = OrderedDict({"0/a": 0, "1/b": 1, "2/c": 2, "3/d": 3})
        out = unflatten_list_dict(in_)
        assert type(out) is list

    def test_unflatten_list_dict_one_level_nested(self):
        result = unflatten_list_dict({"a/0": 0, "a/1": 1, "c/d": 2, "e": 3})
        assert result == {"a": [0, 1], "c": {"d": 2}, "e": 3}

        result = unflatten_list_dict({"0/a": 0, "1/b": 1, "2/c": 2, "3": 3})
        assert result == [{"a": 0}, {"b": 1}, {"c": 2}, 3]

    def test_unflatten_list_dict_multi_level_nested(self):
        result = unflatten_list_dict({"a/0/c/d": 1, "a/1/c": 2, "a/2": 3, "e": 4})
        assert result == {"a": [{"c": {"d": 1}}, {"c": 2}, 3], "e": 4}

        result = unflatten_list_dict(
            {"0/a/0/b": 1, "0/a/1": 2, "1/0": 3, "1/1": 4, "1/2/c": 5, "2": 6}
        )
        assert result == [{"a": [{"b": 1}, 2]}, [3, 4, {"c": 5}], 6]

    def test_unflatten_noop(self):
        """Unflattening an already unflattened dict should be a noop."""
        unflattened = {"a": 1, "b": {"c": {"d": [1, 2]}, "e": 3}, "f": {"g": 3}}
        assert unflattened == unflatten_dict(unflattened)
        assert unflattened == unflatten_list_dict(unflattened)

    def test_raises_error_on_key_conflict(self):
        """Ensure that an informative exception is raised on key conflict."""
        with self.assertRaisesRegex(TypeError, r"Cannot unflatten dict"):
            unflatten_dict({"a": 1, "a/b": 2, "a/c": 3})

        with self.assertRaisesRegex(TypeError, r"Cannot unflatten dict"):
            unflatten_dict({"a/b": 2, "a/b/c": 3})


class GPUUtilMock:
    class GPU:
        def __init__(self, id, uuid, util=None):
            self.id = id
            self.uuid = uuid
            self.util = [0.5, 0.0]

        @property
        def memoryUtil(self):
            if self.util:
                return self.util.pop(0)
            return 0

    def __init__(self, gpus, gpu_uuids):
        self.gpus = gpus
        self.uuids = gpu_uuids
        self.gpu_list = [
            self.GPU(gpu, uuid) for gpu, uuid in zip(self.gpus, self.uuids)
        ]

    def getGPUs(self):
        return self.gpu_list


class GPUTest(unittest.TestCase):
    def setUp(self):
        sys.modules["GPUtil"] = GPUUtilMock([0, 1], ["GPU-aaa", "GPU-bbb"])

    def testGPUWait1(self):
        wait_for_gpu(0, delay_s=0)

    def testGPUWait2(self):
        wait_for_gpu("1", delay_s=0)

    def testGPUWait3(self):
        wait_for_gpu("GPU-aaa", delay_s=0)

    def testGPUWaitFail(self):
        with self.assertRaises(ValueError):
            wait_for_gpu(2, delay_s=0)

        with self.assertRaises(ValueError):
            wait_for_gpu("4", delay_s=0)

        with self.assertRaises(ValueError):
            wait_for_gpu(1.23, delay_s=0)

    @patch("ray.get_gpu_ids", lambda: ["0"])
    def testDefaultGPU(self):
        import sys

        sys.modules["GPUtil"] = GPUUtilMock([0], ["GPU-aaa"])
        wait_for_gpu(delay_s=0)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))

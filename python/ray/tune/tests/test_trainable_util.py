import copy
from collections import OrderedDict
import pytest
import sys
import unittest
from unittest.mock import patch

from ray.tune.utils.util import wait_for_acc
from ray.tune.utils.util import flatten_dict, unflatten_dict, unflatten_list_dict


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


class ACCUtilMock:
    class ACC:
        def __init__(self, id, uuid, util=None):
            self.id = id
            self.uuid = uuid
            self.util = [0.5, 0.0]

        @property
        def memoryUtil(self):
            if self.util:
                return self.util.pop(0)
            return 0

    def __init__(self, accs, acc_uuids):
        self.accs = accs
        self.uuids = acc_uuids
        self.acc_list = [
            self.ACC(acc, uuid) for acc, uuid in zip(self.accs, self.uuids)
        ]

    def getACCs(self):
        return self.acc_list


class ACCTest(unittest.TestCase):
    def setUp(self):
        sys.modules["ACCtil"] = ACCUtilMock([0, 1], ["ACC-aaa", "ACC-bbb"])

    def testACCWait1(self):
        wait_for_acc(0, delay_s=0)

    def testACCWait2(self):
        wait_for_acc("1", delay_s=0)

    def testACCWait3(self):
        wait_for_acc("ACC-aaa", delay_s=0)

    def testACCWaitFail(self):
        with self.assertRaises(ValueError):
            wait_for_acc(2, delay_s=0)

        with self.assertRaises(ValueError):
            wait_for_acc("4", delay_s=0)

        with self.assertRaises(ValueError):
            wait_for_acc(1.23, delay_s=0)

    @patch("ray.get_acc_ids", lambda: ["0"])
    def testDefaultACC(self):
        import sys

        sys.modules["ACCtil"] = ACCUtilMock([0], ["ACC-aaa"])
        wait_for_acc(delay_s=0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

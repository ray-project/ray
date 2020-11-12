from collections import OrderedDict

import unittest

from .util import unflatten_dict


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


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))

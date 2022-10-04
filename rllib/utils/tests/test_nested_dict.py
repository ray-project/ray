import unittest
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.nested_dict import NestedDict


class TestNestedDict(unittest.TestCase):
    def test_basics(self):
        foo_dict = NestedDict()

        # test __setitem__
        foo_dict["a"] = 100
        foo_dict["b", "c"] = 200
        foo_dict["b", "d"] = 300

        # test asdict
        check(foo_dict.asdict(), {"a": 100, "b": {"c": 200, "d": 300}})

        # test __contains__
        self.assertTrue("a" in foo_dict)
        self.assertTrue(("b", "c") in foo_dict)
        self.assertTrue(("b", "d") in foo_dict)
        self.assertFalse("c" in foo_dict)
        self.assertFalse(("b", "e") in foo_dict)

        # test get()
        self.assertEqual(foo_dict.get("a"), 100)
        self.assertEqual(foo_dict.get("b"), {"c": 200, "d": 300})
        self.assertEqual(foo_dict.get(("b", "d")), 300)
        self.assertRaises(KeyError, lambda: foo_dict.get("c"))
        self.assertEqual(foo_dict.get("c", default=400), 400)

        # test __getitem__
        self.assertEqual(foo_dict["a"], 100)
        self.assertEqual(foo_dict["b", "c"], 200)
        self.assertRaises(IndexError, lambda: foo_dict["b"])

        # # test __len__
        # self.assertEqual(len(foo_dict), 3)

        # test __str__
        self.assertEqual(str(foo_dict), str({"a": 100, "b": {"c": 200, "d": 300}}))

        # test __repr__
        self.assertEqual(repr(foo_dict), repr({"a": 100, "b": {"c": 200, "d": 300}}))

        # test keys()
        self.assertEqual(list(foo_dict.keys()), ["a", ("b", "c"), ("b", "d")])

        # test values()
        self.assertEqual(list(foo_dict.values()), [100, 200, 300])

        # test items()
        self.assertEqual(
            list(foo_dict.items()), [("a", 100), (("b", "c"), 200), (("b", "d"), 300)]
        )

        # test __iter__
        self.assertEqual(list(foo_dict), ["a", ("b", "c"), ("b", "d")])

        # test shallow_keys()
        self.assertEqual(list(foo_dict.shallow_keys()), ["a", "b"])

        # test copy()
        foo_dict_copy = foo_dict.copy()
        self.assertEqual(foo_dict_copy.asdict(), foo_dict.asdict())
        self.assertIsNot(foo_dict_copy, foo_dict)

        # test __delitem__
        del foo_dict["a"]
        self.assertEqual(foo_dict.asdict(), {"b": {"c": 200, "d": 300}})
        del foo_dict["b", "c"]
        self.assertEqual(foo_dict.asdict(), {"b": {"d": 300}})

    def test_filter(self):

        dict1 = NestedDict(
            [
                (("foo", "a"), 10),
                (("foo", "b"), 11),
                (("bar", "c"), 11),
                (("bar", "a"), 110),
            ]
        )
        dict2 = NestedDict([("foo", NestedDict(dict(a=11)))])
        dict3 = NestedDict(
            [("foo", NestedDict(dict(a=100))), ("bar", NestedDict(dict(d=11)))]
        )
        dict4 = NestedDict(
            [("foo", NestedDict(dict(a=100))), ("bar", NestedDict(dict(c=11)))]
        )

        self.assertEqual(dict1.filter(dict2).asdict(), {"foo": {"a": 10}})
        self.assertEqual(
            dict1.filter(dict4).asdict(), {"bar": {"c": 11}, "foo": {"a": 10}}
        )
        self.assertRaises(KeyError, lambda: dict1.filter(dict3).asdict())


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import unittest
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.nested_dict import NestedDict


class TestNestedDict(unittest.TestCase):
    def test_basics(self):
        foo_dict = NestedDict()

        # test __setitem__
        def set_invalid_item_1():
            foo_dict[()] = 1

        def set_invalid_item_2():
            foo_dict[""] = 1

        self.assertRaises(IndexError, set_invalid_item_1)
        self.assertRaises(IndexError, set_invalid_item_2)

        desired_dict = {
            "aa": 100,
            "b": {"c": 200, "d": 300},
            "c": {"e": {"f": 400}},
            "d": {"g": {"h": {"i": 500}}},
        }

        desired_keys = [
            ("aa",),
            ("b", "c"),
            ("b", "d"),
            ("c", "e", "f"),
            ("d", "g", "h", "i"),
        ]

        desired_values = [100, 200, 300, 400, 500]

        foo_dict["aa"] = 100
        foo_dict["b", "c"] = 200
        foo_dict[("b", "d")] = 300
        foo_dict["c", "e"] = {"f": 400}

        # Note: key ("c", "f") should not be a valid key since it is empty
        foo_dict["c", "f"] = NestedDict()

        # test __len__
        self.assertEqual(len(foo_dict), len(desired_keys) - 1)

        # test __iter__
        self.assertEqual(list(iter(foo_dict)), desired_keys[:-1])

        # this call will use __len__ and __iter__
        foo_dict["d"] = {"g": NestedDict([(("h"), NestedDict({"i": 500}))])}

        # test asdict
        check(foo_dict.asdict(), desired_dict)

        # test __len__ again
        self.assertEqual(len(foo_dict), len(desired_keys))

        # test __iter__ again
        self.assertEqual(list(iter(foo_dict)), desired_keys)

        # test __contains__
        self.assertTrue("aa" in foo_dict)
        self.assertTrue(("b", "c") in foo_dict)
        self.assertTrue(("b", "c") in foo_dict)
        self.assertTrue(("b", "d") in foo_dict)
        self.assertTrue(("d", "g", ("h", "i")) in foo_dict)
        self.assertFalse("f" in foo_dict)
        self.assertFalse(("b", "e") in foo_dict)

        # test get()
        self.assertEqual(foo_dict.get("aa"), 100)
        self.assertEqual(foo_dict.get("b").asdict(), {"c": 200, "d": 300})
        self.assertEqual(foo_dict.get(("b", "d")), 300)
        self.assertRaises(KeyError, lambda: foo_dict.get("e"))
        self.assertEqual(foo_dict.get("e", default=400), 400)

        # test __getitem__
        self.assertEqual(foo_dict["aa"], 100)
        self.assertEqual(foo_dict["b", "c"], 200)
        self.assertEqual(foo_dict["c", "e", "f"], 400)
        self.assertEqual(foo_dict["d", "g", "h", "i"], 500)
        self.assertRaises(IndexError, lambda: foo_dict["b"])

        # test __str__
        self.assertEqual(str(foo_dict), str(desired_dict))

        # test keys()
        self.assertEqual(list(foo_dict.keys()), desired_keys)

        # test values()
        self.assertEqual(list(foo_dict.values()), desired_values)

        # test items()
        self.assertEqual(
            list(foo_dict.items()), list(zip(desired_keys, desired_values))
        )

        # test shallow_keys()
        self.assertEqual(list(foo_dict.shallow_keys()), ["aa", "b", "c", "d"])

        # test copy()
        foo_dict_copy = foo_dict.copy()
        self.assertEqual(foo_dict_copy.asdict(), foo_dict.asdict())
        self.assertIsNot(foo_dict_copy, foo_dict)

        # test __delitem__
        del foo_dict["d", "g", "h", "i"]
        del desired_dict["d"]["g"]
        self.assertNotEqual(foo_dict.asdict(), desired_dict)

        del desired_dict["d"]
        self.assertEqual(foo_dict.asdict(), desired_dict)

    def test_filter(self):

        dict1 = NestedDict(
            [
                (("foo", "a"), 10),
                (("foo", "b"), 11),
                (("bar", "c"), 11),
                (("bar", "a"), 110),
            ]
        )
        dict2 = NestedDict([("foo", NestedDict(dict(a=33)))])
        dict3 = NestedDict(
            [("foo", NestedDict(dict(a=None))), ("bar", NestedDict(dict(d=None)))]
        )
        dict4 = NestedDict(
            [("foo", NestedDict(dict(a=None))), ("bar", NestedDict(dict(c=None)))]
        )

        self.assertEqual(dict1.filter(dict2).asdict(), {"foo": {"a": 10}})
        self.assertEqual(
            dict1.filter(dict4).asdict(), {"bar": {"c": 11}, "foo": {"a": 10}}
        )
        self.assertRaises(KeyError, lambda: dict1.filter(dict3).asdict())
        self.assertEqual(
            dict1.filter(dict3, ignore_missing=True).asdict(), {"foo": {"a": 10}}
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import unittest

from ray import tune
from ray.tune.impl.placeholder import inject_placeholders, resolve_placeholders


class PlaceholderTest(unittest.TestCase):
    def testGridSearch(self):
        config = {
            "param1": "ok",
            "param2": ["not ok", tune.grid_search(["ok", "not ok"])],
            "param3": {
                "param4": tune.grid_search(["ok", "not ok"]),
            },
        }

        replaced = {}
        config = inject_placeholders(config, replaced)

        self.assertEqual(config["param2"][1]["grid_search"], ["cat_0", "cat_1"])
        self.assertEqual(config["param3"]["param4"]["grid_search"], ["cat_0", "cat_1"])

        # Pretend we picked a choice from the grid searches.
        config["param2"][1] = "cat_0"
        config["param3"]["param4"] = "cat_1"

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][1], "ok")
        self.assertEqual(config["param3"]["param4"], "not ok")

    def testCategorical(self):
        config = {
            "param1": "ok",
            "param2": ["not ok", tune.choice(["ok", "not ok"])],
            "param3": {
                "param4": tune.choice(["ok", "not ok"]),
            },
        }

        replaced = {}
        config = inject_placeholders(config, replaced)

        self.assertEqual(config["param2"][1].categories, ["cat_0", "cat_1"])
        self.assertEqual(config["param3"]["param4"].categories, ["cat_0", "cat_1"])

        # Pretend we picked a choice from the grid searches.
        config["param2"][1] = "cat_0"
        config["param3"]["param4"] = "cat_1"

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][1], "ok")
        self.assertEqual(config["param3"]["param4"], "not ok")

    def testFunction(self):
        config = {
            "param1": "ok",
            "param2": ["not ok", tune.sample_from(lambda: "not ok")],
            # Both lambdas, either taking spec or config, should work.
            "param3": {
                "param4": tune.sample_from(lambda spec: spec["config"]["param1"]),
            },
            "param4": {
                "param4": tune.sample_from(lambda config: config["param1"]),
            },
            # Make sure dot notation also works with spec passed in.
            "param5": {
                "param4": tune.sample_from(lambda spec: spec.config["param1"]),
            },
        }

        replaced = {}
        config = inject_placeholders(config, replaced)

        self.assertEqual(config["param2"][1], "fn_ph")
        self.assertEqual(config["param3"]["param4"], "fn_ph")
        self.assertEqual(config["param4"]["param4"], "fn_ph")
        self.assertEqual(config["param5"]["param4"], "fn_ph")

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][1], "not ok")
        self.assertEqual(config["param3"]["param4"], "ok")
        self.assertEqual(config["param4"]["param4"], "ok")
        self.assertEqual(config["param5"]["param4"], "ok")

    def testRefValue(self):
        class Dummy:
            def __init__(self, value):
                self.value = value

        config = {
            "param1": "ok",
            "param2": ["not ok", Dummy("ok")],
            "param3": {
                "param4": Dummy("not ok"),
            },
        }

        replaced = {}
        config = inject_placeholders(config, replaced)

        self.assertEqual(config["param2"][1], "ref_ph")
        self.assertEqual(config["param3"]["param4"], "ref_ph")

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][1].value, "ok")
        self.assertEqual(config["param3"]["param4"].value, "not ok")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import numpy as np
import unittest

from ray import tune
from ray.tune.impl.placeholder import (
    inject_placeholders,
    resolve_placeholders,
    create_resolvers_map,
    _FunctionResolver,
    _RefResolver,
)
from ray.tune.search.sample import Float, Integer


class Dummy:
    def __init__(self, value):
        self.value = value


class PlaceholderTest(unittest.TestCase):
    def testNotReplaced(self):
        config = {
            "param1": "ok",
            "param2": ["not ok", tune.grid_search(["ok", "not ok"])],
            "param3": {
                "param4": tune.choice(["ok", "not ok"]),
            },
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        # Primitive typed choices are not replaced.
        self.assertEqual(config["param2"][1]["grid_search"], ["ok", "not ok"])
        self.assertEqual(config["param3"]["param4"].categories, ["ok", "not ok"])

    def testGridSearch(self):
        config = {
            "param1": "ok",
            "param2": ["not ok", tune.grid_search(["ok", Dummy("not ok")])],
            "param3": {
                "param4": tune.grid_search([Dummy("ok"), "not ok"]),
            },
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        self.assertEqual(
            config["param2"][1]["grid_search"],
            ["ok", (_RefResolver.TOKEN, "e1eaa08f")],
        )
        self.assertEqual(
            config["param3"]["param4"]["grid_search"],
            [(_RefResolver.TOKEN, "35397f1a"), "not ok"],
        )

        # Pretend we picked a choice from the grid searches.
        config["param2"][1] = (_RefResolver.TOKEN, "e1eaa08f")
        config["param3"]["param4"] = "not ok"

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][1].value, "not ok")
        self.assertEqual(config["param3"]["param4"], "not ok")

    def testCategorical(self):
        config = {
            "param1": "ok",
            "param2": ["not ok", tune.choice([Dummy("ok"), "not ok"])],
            "param3": {
                "param4": tune.choice([Dummy("ok"), "not ok"]),
            },
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        self.assertEqual(
            config["param2"][1].categories,
            [(_RefResolver.TOKEN, "e6a5a3d5"), "not ok"],
        )
        self.assertEqual(
            config["param3"]["param4"].categories,
            [(_RefResolver.TOKEN, "35397f1a"), "not ok"],
        )

        # Pretend we picked a choice from the categoricals.
        config["param2"][1] = (_RefResolver.TOKEN, "e6a5a3d5")
        config["param3"]["param4"] = "not ok"

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][1].value, "ok")
        self.assertEqual(config["param3"]["param4"], "not ok")

    def _testNonSearchSpaceRef(self, value):
        """Tests that non-primitives (numpy, lambda fn) get replaced by a reference."""
        config = {"param": tune.choice([value, "other", value])}

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        self.assertEqual(
            config["param"].categories,
            [
                (_RefResolver.TOKEN, "ab9affa5"),
                "other",
                (_RefResolver.TOKEN, "ceae296d"),
            ],
        )

    def testNumpyToRef(self):
        self._testNonSearchSpaceRef(np.arange(10))

    def testLambdaToRef(self):
        self._testNonSearchSpaceRef(lambda x: x)

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

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        self.assertEqual(config["param2"][1][0], _FunctionResolver.TOKEN)
        self.assertEqual(config["param3"]["param4"][0], _FunctionResolver.TOKEN)
        self.assertEqual(config["param4"]["param4"][0], _FunctionResolver.TOKEN)
        self.assertEqual(config["param5"]["param4"][0], _FunctionResolver.TOKEN)

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][1], "not ok")
        self.assertEqual(config["param3"]["param4"], "ok")
        self.assertEqual(config["param4"]["param4"], "ok")
        self.assertEqual(config["param5"]["param4"], "ok")

    def testRefValue(self):
        config = {
            "param1": "ok",
            "param2": ["not ok", Dummy("ok")],
            "param3": {
                "param4": Dummy("not ok"),
            },
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        self.assertEqual(config["param2"][1][0], _RefResolver.TOKEN)
        self.assertEqual(config["param3"]["param4"][0], _RefResolver.TOKEN)

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][1].value, "ok")
        self.assertEqual(config["param3"]["param4"].value, "not ok")

    def testTuple(self):
        class Dummy:
            def __init__(self, value):
                self.value = value

        config = {
            "param1": ("ok", "not ok"),
            "param2": ["not ok", (1, Dummy("ok"))],
            "param3": {
                "param4": (1, [2, Dummy("not ok")], 3),
            },
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        self.assertTrue(isinstance(config["param1"], tuple))
        self.assertEqual(config["param1"], ("ok", "not ok"))
        self.assertTrue(isinstance(config["param2"][1], tuple))
        self.assertTrue(isinstance(config["param3"]["param4"], tuple))

        resolve_placeholders(config, replaced)

        self.assertTrue(isinstance(config["param2"][1], tuple))
        self.assertEqual(config["param2"][1][1].value, "ok")
        self.assertTrue(isinstance(config["param3"]["param4"], tuple))
        self.assertEqual(config["param3"]["param4"][1][1].value, "not ok")

    def testOtherDomains(self):
        config = {
            "param1": tune.uniform(0, 1),
            "param2": tune.randint(2, 3),
            "param3": tune.qrandn(0, 1, 0.1),
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        # Normal params are not replaced.
        self.assertTrue(isinstance(config["param1"], Float))
        self.assertTrue(isinstance(config["param2"], Integer))
        self.assertTrue(isinstance(config["param3"], Float))

    def testPointToEval(self):
        config = {
            "param1": "ok",
            "param2": ["not ok", tune.choice([Dummy("ok"), "not ok"])],
            "param3": {
                "param4": tune.sample_from(lambda spec: spec["config"]["param1"]),
            },
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        # Normal params are not replaced.
        self.assertEqual(
            config["param2"][1].categories,
            [(_RefResolver.TOKEN, "e6a5a3d5"), "not ok"],
        )
        self.assertEqual(
            config["param3"]["param4"], (_FunctionResolver.TOKEN, "843363f5")
        )

        # Now, say we manually resolved the placeholders based on
        # points_to_evaluate.
        config["param2"][1] = "not_ok"
        config["param3"]["param4"] = "ok"

        resolve_placeholders(config, replaced)

        # Params stays the same.
        self.assertEqual(config["param2"][1], "not_ok")
        self.assertEqual(config["param3"]["param4"], "ok")

    def testSimpleNestedSearchSpaces(self):
        config = {
            "param1": "ok",
            "param2": tune.choice(
                [
                    tune.choice([Dummy(1), 2, 3]),
                    tune.uniform(5, 6),
                ]
            ),
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        # Manually resolve. Select the Dummy value.
        config["param2"] = (_RefResolver.TOKEN, "41821403")

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"].value, 1)

    def testSimpleNestedSearchSpaces2(self):
        config = {
            "param1": "ok",
            "param2": tune.choice(
                [
                    (None, Dummy(1), None),
                    (Dummy(2), None, None),
                    (None, None, Dummy(3)),
                ]
            ),
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        # Manually resolve. Select the Dummy value.
        config["param2"] = (None, None, (_RefResolver.TOKEN, "49964529"))

        resolve_placeholders(config, replaced)

        self.assertEqual(config["param2"][2].value, 3)

    def testResolveFunctionAfterRef(self):
        config = {
            "param1": "ok",
            "param2": tune.choice([Dummy("ok"), "not ok"]),
            "param3": {
                "param4": tune.sample_from(lambda config: config["param2"]),
            },
        }

        replaced = create_resolvers_map()
        config = inject_placeholders(config, replaced)

        # Manually resolve param2.
        config["param2"] = (_RefResolver.TOKEN, "60238385")

        resolve_placeholders(config, replaced)

        # param3.param4 should get the same value as resolved param2.
        self.assertEqual(config["param3"]["param4"].value, "ok")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

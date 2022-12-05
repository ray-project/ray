import unittest

from ray.rllib.algorithms.pg import PGConfig
from ray.rllib.policy.policy import PolicySpec


class TestCheckMultiAgent(unittest.TestCase):
    def test_multi_agent_invalid_args(self):
        self.assertRaisesRegex(
            TypeError,
            "got an unexpected keyword argument 'wrong_key'",
            lambda: (
                PGConfig().multi_agent(
                    policies={"p0"}, policies_to_train=["p0"], wrong_key=1
                )
            ),
        )

    def test_multi_agent_bad_policy_ids(self):
        self.assertRaisesRegex(
            KeyError,
            "Policy IDs must always be of type",
            lambda: (
                PGConfig().multi_agent(
                    policies={1, "good_id"},
                    policy_mapping_fn=lambda agent_id, episode, worker, **kw: "good_id",
                )
            ),
        )

    def test_multi_agent_invalid_sub_values(self):
        self.assertRaisesRegex(
            ValueError,
            "config.multi_agent\\(count_steps_by=..\\) must be one of",
            lambda: (PGConfig().multi_agent(count_steps_by="invalid_value")),
        )

    def test_multi_agent_invalid_override_configs(self):
        self.assertRaisesRegex(
            KeyError,
            "Invalid property name invdli for config class PGConfig",
            lambda: (
                PGConfig().multi_agent(
                    policies={
                        "p0": PolicySpec(config=PGConfig.overrides(invdli=42.0)),
                    }
                )
            ),
        )
        self.assertRaisesRegex(
            KeyError,
            "Invalid property name invdli for config class PGConfig",
            lambda: (
                PGConfig().multi_agent(
                    policies={
                        "p0": PolicySpec(config=PGConfig.overrides(invdli=42.0)),
                    }
                )
            ),
        )

    def test_setting_multi_agent_should_fail(self):
        config = PGConfig().multi_agent(
            policies={
                "pol1": (None, None, None, None),
                "pol2": (None, None, None, PGConfig.overrides(lr=0.001)),
            }
        )

        def set_ma(config):
            print(config["multiagent"])  # ok
            config["multiagent"] = {"policies": {"pol1", "pol2"}}  # not ok

        self.assertRaisesRegex(
            AttributeError,
            "Cannot set `multiagent` key in an AlgorithmConfig!",
            lambda: set_ma(config),
        )


if __name__ == "__main__":
    import pytest

    pytest.main()

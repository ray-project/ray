import unittest

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy.policy import PolicySpec


class TestCheckMultiAgent(unittest.TestCase):
    def test_multi_agent_invalid_args(self):
        self.assertRaisesRegex(
            TypeError,
            "got an unexpected keyword argument 'wrong_key'",
            lambda: (
                PPOConfig().multi_agent(
                    policies={"p0"}, policies_to_train=["p0"], wrong_key=1
                )
            ),
        )

    def test_multi_agent_bad_policy_ids(self):
        self.assertRaisesRegex(
            ValueError,
            "PolicyID `1` not valid!",
            lambda: (
                PPOConfig().multi_agent(
                    policies={1, "good_id"},
                    policy_mapping_fn=lambda agent_id, episode, worker, **kw: "good_id",
                )
            ),
        )

    def test_multi_agent_invalid_sub_values(self):
        self.assertRaisesRegex(
            ValueError,
            "config.multi_agent\\(count_steps_by=..\\) must be one of",
            lambda: (PPOConfig().multi_agent(count_steps_by="invalid_value")),
        )

    def test_multi_agent_invalid_override_configs(self):
        self.assertRaisesRegex(
            KeyError,
            "Invalid property name invdli for config class PPOConfig",
            lambda: (
                PPOConfig().multi_agent(
                    policies={
                        "p0": PolicySpec(config=PPOConfig.overrides(invdli=42.0)),
                    }
                )
            ),
        )
        self.assertRaisesRegex(
            KeyError,
            "Invalid property name invdli for config class PPOConfig",
            lambda: (
                PPOConfig().multi_agent(
                    policies={
                        "p0": PolicySpec(config=PPOConfig.overrides(invdli=42.0)),
                    }
                )
            ),
        )

    def test_setting_multiagent_key_in_config_should_fail(self):
        config = PPOConfig().multi_agent(
            policies={
                "pol1": (None, None, None, None),
                "pol2": (None, None, None, PPOConfig.overrides(lr=0.001)),
            }
        )

        def set_ma(config):
            # not ok: cannot set "multiagent" key in AlgorithmConfig anymore.
            config["multiagent"] = {"policies": {"pol1", "pol2"}}

        self.assertRaisesRegex(
            AttributeError,
            "Cannot set `multiagent` key in an AlgorithmConfig!",
            lambda: set_ma(config),
        )


if __name__ == "__main__":
    import pytest

    pytest.main()

import unittest

from ray.rllib.algorithms.pg import PG


class TestCheckMultiAgent(unittest.TestCase):
    def test_multi_agent_dict_invalid_subkeys(self):
        config = {
            "multiagent": {
                "wrong_key": 1,
                "policies": {"p0"},
                "policies_to_train": ["p0"],
            }
        }
        self.assertRaisesRegex(
            KeyError,
            "You have invalid keys in your",
            lambda: PG(config, env="CartPole-v0"),
        )

    def test_multi_agent_dict_bad_policy_ids(self):
        config = {
            "multiagent": {
                "policies": {1, "good_id"},
                "policy_mapping_fn": lambda aid, **kw: "good_id",
            }
        }
        self.assertRaisesRegex(
            KeyError,
            "Policy IDs must always be of type",
            lambda: PG(config, env="CartPole-v0"),
        )

    def test_multi_agent_dict_invalid_sub_values(self):
        config = {"multiagent": {"count_steps_by": "invalid_value"}}
        self.assertRaisesRegex(
            ValueError,
            "config.multiagent.count_steps_by must be",
            lambda: PG(config, env="CartPole-v0"),
        )


if __name__ == "__main__":
    import pytest

    pytest.main()

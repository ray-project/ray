import gym
import json
import unittest

from ray.rllib.policy.view_requirement import ViewRequirement


class TestViewRequirement(unittest.TestCase):
    def test_serialize_view_requirement(self):
        """Test serializing simple ViewRequirement into JSON serializable dict"""
        vr = ViewRequirement(
            "obs",
            shift=[-1],
            used_for_training=False,
            used_for_compute_actions=True,
            batch_repeat_value=1,
        )
        d = vr.to_dict()
        self.assertEqual(d["data_col"], "obs")
        self.assertEqual(d["space"]["space"], "box")

        # Make sure serialized dict is JSON serializable.
        s = json.dumps(d)
        d2 = json.loads(s)

        self.assertEqual(d2["used_for_training"], False)
        self.assertEqual(d2["used_for_compute_actions"], True)

        vr2 = ViewRequirement.from_dict(d2)
        self.assertEqual(vr2.data_col, "obs")
        self.assertTrue(isinstance(vr2.space, gym.spaces.Box))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

import unittest
import numpy as np
from gym import envs
from gym.envs.tests.spec_list import skip_mujoco


def verify_environments_match(old_environment_id,
                              new_environment_id,
                              seed=1,
                              num_actions=1000):
    old_environment = envs.make(old_environment_id)
    new_environment = envs.make(new_environment_id)

    old_environment.seed(seed)
    new_environment.seed(seed)

    old_reset_observation = old_environment.reset()
    new_reset_observation = new_environment.reset()

    np.testing.assert_allclose(old_reset_observation, new_reset_observation)

    for i in range(num_actions):
        action = old_environment.action_space.sample()
        old_observation, old_reward, old_done, old_info = old_environment.step(
            action)
        new_observation, new_reward, new_done, new_info = new_environment.step(
            action)

        np.testing.assert_allclose(old_observation, new_observation)
        np.testing.assert_allclose(old_reward, new_reward)
        np.testing.assert_allclose(old_done, new_done)

        for key in old_info:
            np.testing.assert_array_equal(old_info[key], new_info[key])


@unittest.skipIf(skip_mujoco, 'Cannot run mujoco key ' +
                              '(either license key not found or ' +
                              'mujoco not installed properly')
class Mujocov2Tov2ConverstionTest(unittest.TestCase):
    def test_environments_match(self):
        test_cases = (
            {
                'old_id': 'Swimmer-v2',
                'new_id': 'Swimmer-v3'
             },
            {
                'old_id': 'Hopper-v2',
                'new_id': 'Hopper-v3'
             },
            {
                'old_id': 'Walker2d-v2',
                'new_id': 'Walker2d-v3'
             },
            {
                'old_id': 'HalfCheetah-v2',
                'new_id': 'HalfCheetah-v3'
             },
            {
                'old_id': 'Ant-v2',
                'new_id': 'Ant-v3'
             },
            {
                'old_id': 'Humanoid-v2',
                'new_id': 'Humanoid-v3'
             },
        )

        for test_case in test_cases:
            verify_environments_match(test_case['old_id'], test_case['new_id'])

        # Raises KeyError because the new envs have extra info
        with self.assertRaises(KeyError):
            verify_environments_match('Swimmer-v3', 'Swimmer-v2')

        # Raises KeyError because the new envs have extra info
        with self.assertRaises(KeyError):
            verify_environments_match('Humanoid-v3', 'Humanoid-v2')

        # Raises KeyError because the new envs have extra info
        with self.assertRaises(KeyError):
            verify_environments_match('Swimmer-v3', 'Swimmer-v2')


if __name__ == '__main__':
    unittest.main()

from gym.envs import algorithmic as alg
import unittest

# All concrete subclasses of AlgorithmicEnv
ALL_ENVS = [
    alg.copy_.CopyEnv, 
    alg.duplicated_input.DuplicatedInputEnv,
    alg.repeat_copy.RepeatCopyEnv,
    alg.reverse.ReverseEnv,
    alg.reversed_addition.ReversedAdditionEnv,
]
ALL_TAPE_ENVS = [env for env in ALL_ENVS 
    if issubclass(env, alg.algorithmic_env.TapeAlgorithmicEnv)]
ALL_GRID_ENVS = [env for env in ALL_ENVS 
    if issubclass(env, alg.algorithmic_env.GridAlgorithmicEnv)]

def imprint(env, input_arr):
    """Monkey-patch the given environment so that when reset() is called, the
    input tape/grid will be set to the given data, rather than being randomly
    generated."""
    env.generate_input_data = lambda _: input_arr

class TestAlgorithmicEnvInteractions(unittest.TestCase):
    """Test some generic behaviour not specific to any particular algorithmic
    environment. Movement, allocation of rewards, etc."""
    CANNED_INPUT = [0, 1]
    ENV_KLS = alg.copy_.CopyEnv
    LEFT, RIGHT = ENV_KLS._movement_idx('left'), ENV_KLS._movement_idx('right')
    def setUp(self):
        self.env = self.ENV_KLS(base=2, chars=True)
        imprint(self.env, self.CANNED_INPUT)

    def test_successful_interaction(self):
        obs = self.env.reset()
        self.assertEqual(obs, 0)
        obs, reward, done, _ = self.env.step([self.RIGHT, 1, 0])
        self.assertEqual(obs, 1)
        self.assertGreater(reward, 0)
        self.assertFalse(done)
        obs, reward, done, _ = self.env.step([self.LEFT, 1, 1])
        self.assertTrue(done)
        self.assertGreater(reward, 0)

    def test_bad_output_fail_fast(self):
        obs = self.env.reset()
        obs, reward, done, _ = self.env.step([self.RIGHT, 1, 1])
        self.assertTrue(done)
        self.assertLess(reward, 0)

    def test_levelup(self):
        obs = self.env.reset()
        # Kind of a hack
        alg.algorithmic_env.AlgorithmicEnv.reward_shortfalls = []
        min_length = self.env.min_length
        for i in range(self.env.last):
            obs, reward, done, _ = self.env.step([self.RIGHT, 1, 0])
            self.assertFalse(done)
            obs, reward, done, _ = self.env.step([self.RIGHT, 1, 1])
            self.assertTrue(done)
            self.env.reset()
            if i < self.env.last-1:
                self.assertEqual(len(alg.algorithmic_env.AlgorithmicEnv.reward_shortfalls), i+1)
            else:
                # Should have leveled up on the last iteration
                self.assertEqual(self.env.min_length, min_length+1)
                self.assertEqual(len(alg.algorithmic_env.AlgorithmicEnv.reward_shortfalls), 0)

    def test_walk_off_the_end(self):
        obs = self.env.reset()
        # Walk off the end
        obs, r, done, _ = self.env.step([self.LEFT, 0, 0])
        self.assertEqual(obs, self.env.base)
        self.assertEqual(r, 0)
        self.assertFalse(done)
        # Walk further off track
        obs, r, done, _ = self.env.step([self.LEFT, 0, 0])
        self.assertEqual(obs, self.env.base)
        self.assertFalse(done)
        # Return to the first input character
        obs, r, done, _ = self.env.step([self.RIGHT, 0, 0])
        self.assertEqual(obs, self.env.base)
        self.assertFalse(done)
        obs, r, done, _ = self.env.step([self.RIGHT, 0, 0])
        self.assertEqual(obs, 0)

    def test_grid_naviation(self):
        env = alg.reversed_addition.ReversedAdditionEnv(rows=2, base=6)
        N,S,E,W = [env._movement_idx(named_dir) for named_dir in ['up', 'down', 'right', 'left']]
        # Corresponds to a grid that looks like...
        #       0 1 2
        #       3 4 5
        canned = [ [0, 3], [1, 4], [2, 5] ]
        imprint(env, canned)
        obs = env.reset()
        self.assertEqual(obs, 0)
        navigation = [
          (S, 3), (N, 0), (E, 1), (S, 4), (S, 6), (E, 6), (N, 5), (N, 2), (W, 1)
        ]
        for (movement, expected_obs) in navigation:
            obs, reward, done, _ = env.step([movement, 0, 0])
            self.assertEqual(reward, 0)
            self.assertFalse(done)
            self.assertEqual(obs, expected_obs)

    def test_grid_success(self):
        env = alg.reversed_addition.ReversedAdditionEnv(rows=2, base=3)
        canned = [ [1, 2], [1, 0], [2, 2] ]
        imprint(env, canned)
        obs = env.reset()
        target = [0, 2, 1, 1]
        self.assertEqual(env.target, target)
        self.assertEqual(obs, 1)
        for i, target_digit in enumerate(target):
            obs, reward, done, _ = env.step([0, 1, target_digit])
            self.assertGreater(reward, 0)
            self.assertEqual(done, i==len(target)-1)

    def test_sane_time_limit(self):
        obs = self.env.reset()
        self.assertLess(self.env.time_limit, 100)
        for _ in range(100):
            obs, r, done, _ = self.env.step([self.LEFT, 0, 0])
            if done:
                return
        self.fail("Time limit wasn't enforced")

    def test_rendering(self):
        env = self.env
        obs = env.reset()
        self.assertEqual(env._get_str_obs(), 'A')
        self.assertEqual(env._get_str_obs(1), 'B')
        self.assertEqual(env._get_str_obs(-1), ' ')
        self.assertEqual(env._get_str_obs(2), ' ')
        self.assertEqual(env._get_str_target(0), 'A')
        self.assertEqual(env._get_str_target(1), 'B')
        # Test numerical alphabet rendering
        env = self.ENV_KLS(base=3, chars=False)
        imprint(env, self.CANNED_INPUT)
        env.reset()
        self.assertEqual(env._get_str_obs(), '0')
        self.assertEqual(env._get_str_obs(1), '1')


class TestTargets(unittest.TestCase):
    """Test the rules mapping input strings/grids to target outputs."""
    def test_reverse_target(self):
        input_expected = [
            ([0], [0]),
            ([0, 1], [1, 0]),
            ([1, 1], [1, 1]),
            ([1, 0, 1], [1, 0, 1]),
            ([0, 0, 1, 1], [1, 1, 0, 0]),
        ]
        env = alg.reverse.ReverseEnv()
        for input_arr, expected in input_expected:
            target = env.target_from_input_data(input_arr)
            self.assertEqual(target, expected)

    def test_reversed_addition_target(self):
        env = alg.reversed_addition.ReversedAdditionEnv(base=3)
        input_expected = [
            ([[1,1], [1,1]], [2, 2]),
            ([[2,2], [0,1]], [1, 2]),
            ([[2,1], [1,1], [1,1], [1,0]], [0, 0, 0, 2]),
        ]
        for (input_grid, expected_target) in input_expected:
            self.assertEqual(env.target_from_input_data(input_grid), expected_target)

    def test_reversed_addition_3rows(self):
        env = alg.reversed_addition.ReversedAdditionEnv(base=3, rows=3)
        input_expected = [
            ([[1,1,0],[0,1,1]], [2, 2]),
            ([[1,1,2],[0,1,1]], [1,0,1]),
        ]
        for (input_grid, expected_target) in input_expected:
            self.assertEqual(env.target_from_input_data(input_grid), expected_target)

    def test_copy_target(self):
        env = alg.copy_.CopyEnv()
        self.assertEqual(env.target_from_input_data([0, 1, 2]), [0, 1, 2])

    def test_duplicated_input_target(self):
        env = alg.duplicated_input.DuplicatedInputEnv(duplication=2)
        self.assertEqual(env.target_from_input_data([0, 0, 0, 0, 1, 1]), [0, 0, 1])

    def test_repeat_copy_target(self):
        env = alg.repeat_copy.RepeatCopyEnv()
        self.assertEqual(env.target_from_input_data([0, 1, 2]), [0, 1, 2, 2, 1, 0, 0, 1, 2])

class TestInputGeneration(unittest.TestCase):
    """Test random input generation.
    """
    def test_tape_inputs(self):
        for env_kls in ALL_TAPE_ENVS:
            env = env_kls()
            for size in range(2,5):
                input_tape = env.generate_input_data(size)
                self.assertTrue(all(0<=x<=env.base for x in input_tape),
                "Invalid input tape from env {}: {}".format(env_kls, input_tape))
                # DuplicatedInput needs to generate inputs with even length,
                # so it may be short one
                self.assertLessEqual(len(input_tape), size)

    def test_grid_inputs(self):
        for env_kls in ALL_GRID_ENVS:
            env = env_kls()
            for size in range(2, 5):
                input_grid = env.generate_input_data(size)
                # Should get "size" sublists, each of length self.rows (not the
                # opposite, as you might expect)
                self.assertEqual(len(input_grid), size)
                self.assertTrue(all(len(col) == env.rows for col in input_grid))
                self.assertTrue(all(0<=x<=env.base for x in input_grid[0]))

    def test_duplicatedinput_inputs(self):
        """The duplicated_input env needs to generate strings with the appropriate
        amount of repetiion."""
        env = alg.duplicated_input.DuplicatedInputEnv(duplication=2)
        input_tape = env.generate_input_data(4)
        self.assertEqual(len(input_tape), 4)
        self.assertEqual(input_tape[0], input_tape[1])
        self.assertEqual(input_tape[2], input_tape[3])
        # If requested input size isn't a multiple of duplication, go lower
        input_tape = env.generate_input_data(3)
        self.assertEqual(len(input_tape), 2)
        self.assertEqual(input_tape[0], input_tape[1])
        # If requested input size is *less than* duplication, go up
        input_tape = env.generate_input_data(1)
        self.assertEqual(len(input_tape), 2)
        self.assertEqual(input_tape[0], input_tape[1])
        
        env = alg.duplicated_input.DuplicatedInputEnv(duplication=3)
        input_tape = env.generate_input_data(6)
        self.assertEqual(len(input_tape), 6)
        self.assertEqual(input_tape[0], input_tape[1])
        self.assertEqual(input_tape[1], input_tape[2])

if __name__ == '__main__':
    unittest.main()

import gym
from ray.rllib.algorithms.ppo import PPOConfig


# Define your problem using python and openAI's gym API:
class ParrotEnv(gym.Env):
    """Environment in which an agent must learn to repeat the seen observations.

    Observations are float numbers indicating the to-be-repeated values,
    e.g. -1.0, 5.1, or 3.2.

    The action space is always the same as the observation space.

    Rewards are r=-abs(observation - action), for all steps.
    """

    def __init__(self, config):
        # Make the space (for actions and observations) configurable.
        self.action_space = config.get(
            "parrot_shriek_range", gym.spaces.Box(-1.0, 1.0, shape=(1,))
        )
        # Since actions should repeat observations, their spaces must be the
        # same.
        self.observation_space = self.action_space
        self.cur_obs = None
        self.episode_len = 0

    def reset(self):
        """Resets the episode and returns the initial observation of the new one."""
        # Reset the episode len.
        self.episode_len = 0
        # Sample a random number from our observation space.
        self.cur_obs = self.observation_space.sample()
        # Return initial observation.
        return self.cur_obs

    def step(self, action):
        """Takes a single step in the episode given `action`

        Returns: New observation, reward, done-flag, info-dict (empty).
        """
        # Set `done` flag after 10 steps.
        self.episode_len += 1
        done = self.episode_len >= 10
        # r = -abs(obs - action)
        reward = -sum(abs(self.cur_obs - action))
        # Set a new observation (random sample).
        self.cur_obs = self.observation_space.sample()
        return self.cur_obs, reward, done, {}


# Create an RLlib Algorithm instance from a PPOConfig to learn how to
# act in the above environment.
config = (
    PPOConfig().environment(
        # Env class to use (here: our gym.Env sub-class from above).
        env=ParrotEnv,
        # Config dict to be passed to our custom env's constructor.
        env_config={"parrot_shriek_range": gym.spaces.Box(-5.0, 5.0, (1,))},
    )
    # Parallelize environment rollouts.
    .rollouts(num_rollout_workers=3)
)
algo = config.build()

# Train for n iterations and report results (mean episode rewards).
# Since we have to guess 10 times and the optimal reward is 0.0
# (exact match between observation and action value),
# we can expect to reach an optimal episode reward of 0.0.
for i in range(5):
    results = algo.train()
    print(f"Iter: {i}; avg. reward={results['episode_reward_mean']}")

# Perform inference (action computations) based on given env observations.
# Note that we are using a slightly simpler env here (-3.0 to 3.0, instead
# of -5.0 to 5.0!), however, this should still work as the agent has
# (hopefully) learned to "just always repeat the observation!".
env = ParrotEnv({"parrot_shriek_range": gym.spaces.Box(-3.0, 3.0, (1,))})
# Get the initial observation (some value between -10.0 and 10.0).
obs = env.reset()
done = False
total_reward = 0.0
# Play one episode.
while not done:
    # Compute a single action, given the current observation
    # from the environment.
    action = algo.compute_single_action(obs)
    # Apply the computed action in the environment.
    obs, reward, done, info = env.step(action)
    # Sum up rewards for reporting purposes.
    total_reward += reward
# Report results.
print(f"Played 1 episode; total-reward={total_reward}")

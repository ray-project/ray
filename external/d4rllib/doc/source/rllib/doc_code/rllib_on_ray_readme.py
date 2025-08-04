# __quick_start_begin__
import gymnasium as gym
import numpy as np
import torch
from typing import Dict, Tuple, Any, Optional

from ray.rllib.algorithms.ppo import PPOConfig


# Define your problem using python and Farama-Foundation's gymnasium API:
class SimpleCorridor(gym.Env):
    """Corridor environment where an agent must learn to move right to reach the exit.

    ---------------------
    | S | 1 | 2 | 3 | G |   S=start; G=goal; corridor_length=5
    ---------------------

    Actions:
        0: Move left
        1: Move right

    Observations:
        A single float representing the agent's current position (index)
        starting at 0.0 and ending at corridor_length

    Rewards:
        -0.1 for each step
        +1.0 when reaching the goal

    Episode termination:
        When the agent reaches the goal (position >= corridor_length)
    """

    def __init__(self, config):
        self.end_pos = config["corridor_length"]
        self.cur_pos = 0.0
        self.action_space = gym.spaces.Discrete(2)  # 0=left, 1=right
        self.observation_space = gym.spaces.Box(0.0, self.end_pos, (1,), np.float32)

    def reset(
        self, *, seed: Optional[int] = None, options: Optional[Dict] = None
    ) -> Tuple[np.ndarray, Dict]:
        """Reset the environment for a new episode.

        Args:
            seed: Random seed for reproducibility
            options: Additional options (not used in this environment)

        Returns:
            Initial observation of the new episode and an info dict.
        """
        super().reset(seed=seed)  # Initialize RNG if seed is provided
        self.cur_pos = 0.0
        # Return initial observation.
        return np.array([self.cur_pos], np.float32), {}

    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """Take a single step in the environment based on the provided action.

        Args:
            action: 0 for left, 1 for right

        Returns:
            A tuple of (observation, reward, terminated, truncated, info):
                observation: Agent's new position
                reward: Reward from taking the action (-0.1 or +1.0)
                terminated: Whether episode is done (reached goal)
                truncated: Whether episode was truncated (always False here)
                info: Additional information (empty dict)
        """
        # Walk left if action is 0 and we're not at the leftmost position
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        # Walk right if action is 1
        elif action == 1:
            self.cur_pos += 1
        # Set `terminated` flag when end of corridor (goal) reached.
        terminated = self.cur_pos >= self.end_pos
        truncated = False
        # +1 when goal reached, otherwise -0.1.
        reward = 1.0 if terminated else -0.1
        return np.array([self.cur_pos], np.float32), reward, terminated, truncated, {}


# Create an RLlib Algorithm instance from a PPOConfig object.
print("Setting up the PPO configuration...")
config = (
    PPOConfig().environment(
        # Env class to use (our custom gymnasium environment).
        SimpleCorridor,
        # Config dict passed to our custom env's constructor.
        # Use corridor with 20 fields (including start and goal).
        env_config={"corridor_length": 20},
    )
    # Parallelize environment rollouts for faster training.
    .env_runners(num_env_runners=3)
    # Use a smaller network for this simple task
    .training(model={"fcnet_hiddens": [64, 64]})
)

# Construct the actual PPO algorithm object from the config.
algo = config.build_algo()
rl_module = algo.get_module()

# Train for n iterations and report results (mean episode rewards).
# Optimal reward calculation:
# - Need at least 19 steps to reach the goal (from position 0 to 19)
# - Each step (except last) gets -0.1 reward: 18 * (-0.1) = -1.8
# - Final step gets +1.0 reward
# - Total optimal reward: -1.8 + 1.0 = -0.8
print("\nStarting training loop...")
for i in range(5):
    results = algo.train()

    # Log the metrics from training results
    print(f"Iteration {i+1}")
    print(f"  Training metrics: {results['env_runners']}")

# Save the trained algorithm (optional)
checkpoint_dir = algo.save()
print(f"\nSaved model checkpoint to: {checkpoint_dir}")

print("\nRunning inference with the trained policy...")
# Create a test environment with a shorter corridor to verify the agent's behavior
env = SimpleCorridor({"corridor_length": 10})
# Get the initial observation (should be: [0.0] for the starting position).
obs, info = env.reset()
terminated = truncated = False
total_reward = 0.0
step_count = 0

# Play one episode and track the agent's trajectory
print("\nAgent trajectory:")
positions = [float(obs[0])]  # Track positions for visualization

while not terminated and not truncated:
    # Compute an action given the current observation
    action_logits = rl_module.forward_inference(
        {"obs": torch.from_numpy(obs).unsqueeze(0)}
    )["action_dist_inputs"].numpy()[
        0
    ]  # [0]: Batch dimension=1

    # Get the action with highest probability
    action = np.argmax(action_logits)

    # Log the agent's decision
    action_name = "LEFT" if action == 0 else "RIGHT"
    print(f"  Step {step_count}: Position {obs[0]:.1f}, Action: {action_name}")

    # Apply the computed action in the environment
    obs, reward, terminated, truncated, info = env.step(action)
    positions.append(float(obs[0]))

    # Sum up rewards
    total_reward += reward
    step_count += 1

# Report final results
print(f"\nEpisode complete:")
print(f"  Steps taken: {step_count}")
print(f"  Total reward: {total_reward:.2f}")
print(f"  Final position: {obs[0]:.1f}")

# Verify the agent has learned the optimal policy
if total_reward > -0.5 and obs[0] >= 9.0:
    print("  Success! The agent has learned the optimal policy (always move right).")
# __quick_start_end__

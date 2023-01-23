import argparse
import gymnasium as gym
import numpy as np
import ray
from gymnasium.spaces import Box, Discrete

from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env import make_multi_agent

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--multi-agent", action="store_true")
parser.add_argument("--stop-iters", type=int, default=10)
parser.add_argument("--stop-timesteps", type=int, default=10000)
parser.add_argument("--stop-reward", type=float, default=9.0)


class CustomRenderedEnv(gym.Env):
    """Example of a custom env, for which you can specify rendering behavior."""

    # Must specify, which render modes are supported by your custom env.
    # For RLlib to render your env via the "render_env" config key, only
    # mode="rgb_array" is needed. RLlib will automatically produce a simple
    # viewer for the returned RGB-images for mode="human", such that you don't
    # have to provide your own window+render handling.
    metadata = {
        "render.modes": ["rgb_array"],
    }

    def __init__(self, config):
        self.end_pos = config.get("corridor_length", 10)
        self.max_steps = config.get("max_steps", 100)
        self.cur_pos = 0
        self.steps = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 999.0, shape=(1,), dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        self.cur_pos = 0.0
        self.steps = 0
        return [self.cur_pos], {}

    def step(self, action):
        self.steps += 1
        assert action in [0, 1], action
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1.0
        elif action == 1:
            self.cur_pos += 1.0
        truncated = self.steps >= self.max_steps
        done = self.cur_pos >= self.end_pos or truncated
        return [self.cur_pos], 10.0 if done else -0.1, done, truncated, {}

    def render(self, mode="rgb"):
        """Implements rendering logic for this env (given current state).

        You can either return an RGB image:
        np.array([height, width, 3], dtype=np.uint8) or take care of
        rendering in a window yourself here (return True then).
        For RLlib, though, only mode=rgb (returning an image) is needed,
        even when "render_env" is True in the RLlib config.

        Args:
            mode: One of "rgb", "human", or "ascii". See gym.Env for
                more information.

        Returns:
            Union[np.ndarray, bool]: An image to render or True (if rendering
                is handled entirely in here).
        """

        # Just generate a random image here for demonstration purposes.
        # Also see `gym/envs/classic_control/cartpole.py` for
        # an example on how to use a Viewer object.
        return np.random.randint(0, 256, size=(300, 400, 3), dtype=np.uint8)


MultiAgentCustomRenderedEnv = make_multi_agent(lambda config: CustomRenderedEnv(config))

if __name__ == "__main__":
    # Note: Recording and rendering in this example
    # should work for both local_mode=True|False.
    ray.init(num_cpus=4)
    args = parser.parse_args()

    # Example config switching on rendering.
    config = (
        PPOConfig()
        # Also try common gym envs like: "CartPole-v1" or "Pendulum-v1".
        .environment(
            MultiAgentCustomRenderedEnv if args.multi_agent else CustomRenderedEnv,
            env_config={"corridor_length": 10, "max_steps": 100},
        )
        .framework(args.framework)
        # Use a vectorized env with 2 sub-envs.
        .rollouts(num_envs_per_worker=2, num_rollout_workers=1)
        .evaluation(
            # Evaluate once per training iteration.
            evaluation_interval=1,
            # Run evaluation on (at least) two episodes
            evaluation_duration=2,
            # ... using one evaluation worker (setting this to 0 will cause
            # evaluation to run on the local evaluation worker, blocking
            # training until evaluation is done).
            evaluation_num_workers=1,
            # Special evaluation config. Keys specified here will override
            # the same keys in the main config, but only for evaluation.
            evaluation_config=PPOConfig.overrides(
                # Render the env while evaluating.
                # Note that this will always only render the 1st RolloutWorker's
                # env and only the 1st sub-env in a vectorized env.
                render_env=True,
            ),
        )
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop),
    ).fit()

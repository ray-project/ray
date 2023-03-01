import gymnasium as gym
import grpc
import numpy as np

import ray.rllib.examples.grpc_cartpole.cartpole_pb2 as cartpole_pb2
import ray.rllib.examples.grpc_cartpole.cartpole_pb2_grpc as cartpole_pb2_grpc

class CartPoleEnv(gym.Env):
    def __init__(self, env_config=None):
        ip = env_config.get("ip", "localhost")
        port = env_config.get("port")
        if port is None:
            raise ValueError("Port is not specified")
        self.channel = grpc.insecure_channel(f"{ip}:{port}")
        self.client = cartpole_pb2_grpc.CartPoleStub(self.channel)

        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(
            low=-np.array([float("inf")] * 4),
            high=np.array([float("inf")] * 4),
            dtype=np.float32,
        )

        self._server_process = env_config.get("server_process")

        self.state = None

    def reset(self, *, seed=None, options=None):
        state = self.client.reset(cartpole_pb2.Empty())
        self.state = np.array([
            state.cart_position,
            state.cart_velocity,
            state.pole_angle,
            state.pole_velocity,
        ], dtype=np.float32)
        return self.state, {}

    def step(self, action):
        step_result = self.client.step(cartpole_pb2.Action(action=int(action)))
        next_state = np.array([
            step_result.state.cart_position,
            step_result.state.cart_velocity,
            step_result.state.pole_angle,
            step_result.state.pole_velocity,
        ], dtype=np.float32)
        reward = step_result.reward
        terminated = step_result.terminated
        truncated = step_result.truncated
        info = {}
        self.state = next_state
        return next_state, reward, terminated, truncated, info

    def render(self, mode='human'):
        pass

    def close(self):
        self.channel.close()

    def __del__(self):
        """For GC, kill the server process corresponding to this env."""
        if self._server_process is not None:
            self._server_process.terminate()
            self._server_process.wait()


if __name__ == "__main__":
    
    env = CartPoleEnv()
    obs = env.reset()
    print(f"obs: {obs}")
    for _ in range(1000):
        obs, reward, done, info = env.step(env.action_space.sample())
        print(f"obs: {obs}, reward: {reward}, done: {done}, info: {info}")

    env.close()
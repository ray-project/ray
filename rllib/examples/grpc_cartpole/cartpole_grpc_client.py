import gymnasium as gym
import grpc
import numpy as np
import time
import atexit

import ray.rllib.examples.grpc_cartpole.cartpole_pb2 as cartpole_pb2
import ray.rllib.examples.grpc_cartpole.cartpole_pb2_grpc as cartpole_pb2_grpc


class CartPoleEnv(gym.Env):
    def __init__(self, env_config=None):
        ip = env_config.get("ip", "localhost")
        port = env_config.get("port")

        # maximum time to wait for server to start, in seconds
        max_timeout = env_config.get("max_timeout", 30)
        # maximum time to wait for channel to be ready, in seconds
        channel_timeout = env_config.get("channel_timeout", 1)

        # self._server_process = env_config.get("server_process")
        self.state = None

        if port is None:
            raise ValueError("Port is not specified")

        self.channel = grpc.insecure_channel(f"{ip}:{port}")
        self.client = cartpole_pb2_grpc.CartPoleStub(self.channel)

        # Wait for the server to become ready
        start_time = time.time()
        spent_time = 0
        while spent_time < max_timeout:
            try:
                grpc.channel_ready_future(self.channel).result(timeout=channel_timeout)
                print("server is ready")
                break
            except grpc.FutureTimeoutError:
                print("Waiting for server to start...")

            spent_time = time.time() - start_time
            print(spent_time)

        if spent_time >= max_timeout:
            # if self._server_process is not None:
            #     self._server_process.terminate()
            #     self._server_process.wait()

            raise TimeoutError("Server did not start within the specified timeout.")

        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(
            low=-np.array([float("inf")] * 4),
            high=np.array([float("inf")] * 4),
            dtype=np.float32,
        )

    def reset(self, *, seed=None, options=None):
        state = self.client.reset(cartpole_pb2.Empty())
        self.state = np.array(
            [
                state.cart_position,
                state.cart_velocity,
                state.pole_angle,
                state.pole_velocity,
            ],
            dtype=np.float32,
        )
        return self.state, {}

    def step(self, action):
        step_result = self.client.step(cartpole_pb2.Action(action=int(action)))
        next_state = np.array(
            [
                step_result.state.cart_position,
                step_result.state.cart_velocity,
                step_result.state.pole_angle,
                step_result.state.pole_velocity,
            ],
            dtype=np.float32,
        )
        reward = step_result.reward
        terminated = step_result.terminated
        truncated = step_result.truncated
        info = {}
        self.state = next_state
        return next_state, reward, terminated, truncated, info

    def render(self, mode="human"):
        pass

    def close(self):
        self.channel.close()

    # def cleanup(self):
    #     """kill the server process corresponding to this env."""
    #     print("cleaning up")
    #     if self._server_process is not None:
    #         self._server_process.terminate()
    #         self._server_process.wait()


if __name__ == "__main__":

    env = CartPoleEnv()
    obs = env.reset()
    print(f"obs: {obs}")
    for _ in range(1000):
        obs, reward, done, info = env.step(env.action_space.sample())
        print(f"obs: {obs}, reward: {reward}, done: {done}, info: {info}")

    env.close()

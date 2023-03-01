from concurrent import futures
import grpc
import gymnasium as gym
import argparse
import signal
import threading
import time
import os

import ray.rllib.examples.grpc_cartpole.cartpole_pb2 as cartpole_pb2
import ray.rllib.examples.grpc_cartpole.cartpole_pb2_grpc as cartpole_pb2_grpc


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--parent-pid", type=str, default=None)
    return parser.parse_args()


class CartPoleEnv(cartpole_pb2_grpc.CartPoleServicer):
    """This uses the old API of gym. but with the new underlying env."""

    def __init__(self):
        self.env = gym.make("CartPole-v1")

    def reset(self, request, context):
        print("server reset")
        state, _ = self.env.reset()
        return cartpole_pb2.State(
            cart_position=state[0],
            cart_velocity=state[1],
            pole_angle=state[2],
            pole_velocity=state[3],
        )

    def step(self, request, context):
        print("server step")
        action = request.action
        state, reward, terminated, truncated, _ = self.env.step(action)
        return cartpole_pb2.StepResult(
            state=cartpole_pb2.State(
                cart_position=state[0],
                cart_velocity=state[1],
                pole_angle=state[2],
                pole_velocity=state[3],
            ),
            reward=reward,
            terminated=terminated,
            truncated=truncated,
        )


def start_self_destroying_server(parent_pid=None):
    if parent_pid is None:
        return
    print("started the self destroying monitor hook.")
    # Set the PID to check
    pid_to_check = parent_pid

    # Define the function that checks the PID
    def check_pid():
        while True:
            time.sleep(1)  # Wait for 1 second
            if not os.path.exists("/proc/" + str(pid_to_check)):  # Check if PID exists
                os.kill(os.getpid(), signal.SIGTERM)  # Kill the current process

    # Start the thread that checks the PID
    pid_thread = threading.Thread(target=check_pid)
    pid_thread.start()


def serve(pargs):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    cartpole_pb2_grpc.add_CartPoleServicer_to_server(CartPoleEnv(), server)
    server.add_insecure_port(f"{pargs.ip}:{pargs.port}")
    server.start()
    print("Server started at port ", pargs.port)
    start_self_destroying_server(pargs.parent_pid)
    server.wait_for_termination()


if __name__ == "__main__":
    serve(_parse_args())

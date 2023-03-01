from concurrent import futures
import grpc
import gymnasium as gym
import argparse

import ray.rllib.examples.grpc_cartpole.cartpole_pb2 as cartpole_pb2
import ray.rllib.examples.grpc_cartpole.cartpole_pb2_grpc as cartpole_pb2_grpc


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=50051)
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


def serve(pargs):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    cartpole_pb2_grpc.add_CartPoleServicer_to_server(CartPoleEnv(), server)
    server.add_insecure_port(f"{pargs.ip}:{pargs.port}")
    server.start()
    print("Server started at port ", pargs.port)
    server.wait_for_termination()


if __name__ == "__main__":
    serve(_parse_args())

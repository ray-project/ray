import subprocess
from pathlib import Path
import time
import os

import ray
from ray.rllib.examples.grpc_cartpole.cartpole_grpc_client import CartPoleEnv
from ray.rllib.algorithms.ppo import PPOConfig
from ray import air, tune


class EnvCreatorOnGRPC:
    def __init__(self, start_port) -> None:
        self._port = start_port
        self._ip = ray.util.get_node_ip_address()

    def __call__(self, env_config=None):
        # Start the server

        # use the current path as the working directory
        path = "rllib/examples/grpc_cartpole/cartpole_grpc_server.py"
        port = getattr(env_config, "worker_index", 0) + self._port
        command = f"python {path} --port {port} --parent-pid {os.getpid()}"
        server_process = subprocess.Popen(command.split(" "))
        # Create the client
        env = CartPoleEnv(
            {"ip": "localhost", "port": port, "server_process": server_process}
        )
        return env


if __name__ == "__main__":

    tune.register_env("CartPoleGRPCEnv", lambda c: EnvCreatorOnGRPC(30000)(c))
    config = (
        PPOConfig()
        .framework("torch")
        .environment("CartPoleGRPCEnv")
        .training(train_batch_size=8192)
        .rollouts(num_rollout_workers=64)
    )

    # results = tune.Tuner(
    #     "PPO",
    #     param_space=config,
    #     run_config=air.RunConfig(stop={"training_iteration": 1}),
    # ).fit()
    algo = config.build()
    algo.train()

    breakpoint()

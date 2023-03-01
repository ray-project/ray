import subprocess
from pathlib import Path
import time


from ray.rllib.examples.grpc_cartpole.cartpole_grpc_client import CartPoleEnv
from ray.rllib.algorithms.ppo import PPOConfig
from ray import air, tune


class EnvCreatorOnGRPC:

    def __init__(self, start_port) -> None:
        self._port = start_port
    
    def __call__(self, env_config=None):
        # Start the server

        # use the current path as the working directory
        path = Path(__file__).parent.absolute() / "cartpole_grpc_server.py"
        port = getattr(env_config, "worker_index", 0) + self._port
        server_process = subprocess.Popen(["python", str(path), "--port", str(port)])

        time.sleep(1)

        # Create the client
        env = CartPoleEnv(
            {"ip": "localhost", "port": port, "server_process": server_process}
        )
        return env
        

if __name__ == "__main__":

    tune.register_env("CartPoleGRPCEnv", lambda c: EnvCreatorOnGRPC(50051)(c))
    config = (
        PPOConfig()
        .framework("torch")
        .environment("CartPoleGRPCEnv")
        # .training(train_batch_size=32768)
        .rollouts(num_rollout_workers=2)
    )

    results = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=air.RunConfig(stop={"training_iteration": 10})
    ).fit()

    breakpoint()
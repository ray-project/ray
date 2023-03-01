This example folder shows how we can create a env server that serves a CartPole simulator using grpc and protobuf. 


First, we can create a new proto file for the CartPole environment. This file will describe what the seralized message would look like. Here's an example of what the proto file might look like:

```
syntax = "proto3";

package cartpole;

service CartPole {
    rpc reset (Empty) returns (State) {}
    rpc step (Action) returns (StepResult) {}
}

message Empty {}

message Action {
    int32 action = 1;
}

message State {
    float cart_position = 1;
    float cart_velocity = 2;
    float pole_angle = 3;
    float pole_velocity = 4;
}

message StepResult {
    State state = 1;
    float reward = 2;
    bool terminated = 3;
    bool truncated = 4;
}
```

This proto file defines a gRPC service called `CartPole` with two RPC methods: `reset` and `step`. The `reset` method takes no arguments and returns a `State` message. The `step` method takes an `Action` message as input and returns a StepResult message.

Next, we need to generate the gRPC stubs from the proto file using the protobuf compiler. You can do this by running the following command:

```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. cartpole.proto
```

This will generate two Python files: `cartpole_pb2.py` and `cartpole_pb2_grpc.py`. The `cartpole_pb2.py` file contains the definitions for the protobuf messages, and the `cartpole_pb2_grpc.py` file contains the client and server stubs for the CartPole service.

Now you can create the server for the `CartPole` service. Here's an example implementation:


```
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

    def __init__(self):
        self.env = gym.make("CartPole-v1")

    def reset(self, request, context):
        print("Server reset")
        state, _ = self.env.reset()
        return cartpole_pb2.State(
            cart_position=state[0],
            cart_velocity=state[1],
            pole_angle=state[2],
            pole_velocity=state[3],
        )

    def step(self, request, context):
        print("Server step")
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
    print("Started the self destroying monitor hook.")
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

```

This script defines a server class that inherits `cartpole_pb2_grpc.CartPoleServicer`. It implements reset and step methods to return the protobuf messages defined in the `cartpole_pb2` module. The serve function then serves this class as a grpc server with a specified IP address and port number. Additionally, this server accepts a parent process ID as an input argument, which allows it to monitor the health of the process and self-destruct if the parent PID is dead. This helps to free up resources consumed by the process.

To elaborate further, the `start_self_destroying_server` function defines a monitor hook that starts a thread to continuously check the status of the parent process ID. If the parent process ID is not found, the current process ID is terminated using the `os.kill` function.

Finally, the serve function adds the `CartPoleEnv` class to the grpc server and starts the server on the specified IP address and port number using grpc.server function. The server then waits for incoming requests using server.`wait_for_termination()`. If a parent PID is provided, the `start_self_destroying_server` function is called to monitor the parent process ID and self-destruct if it is dead. 
And, here's an example implementation of the `CartPole` environment as an OpenAI Gym class that uses gRPC to communicate with the server:

```
import gymnasium as gym
import grpc
import numpy as np
import time

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
                print(f"[spent_time = {spent_time:.3f}] Waiting for server to start...")

            spent_time = time.time() - start_time

        if spent_time >= max_timeout:
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
```

This implementation creates a `CartPoleEnv` class that inherits from the `gym.Env` base class. It initializes the gRPC client in the constructor and defines the `reset` and `step` methods to communicate with the server using the generated stubs. The `observation_space` and `action_space` attributes define the observation and action spaces for the environment. The `render` and `close` methods are left empty since they are not necessary for this particular environment.

You can use this class just like any other OpenAI Gym environment, for example:

```
env = CartPoleEnv({"ip": "localhost", "port": "50051"})
obs, _ = env.reset()
print(f"obs: {obs}")
for _ in range(1000):
    obs, reward, terminated, truncated, info = env.step(env.action_space.sample())
    print(
        f"obs: {obs}, reward: {reward}, terminated: {terminated}, truncated: {truncated}, info: {info}"
    )

env.close()
```

Note that you'll need to have the server running in the background before using this environment class.

In the end, for running RLlib with this environement, we can create a env_creator that will launch the server process and will return the client env with the correct configurations. 

```
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
        env = CartPoleEnv({"ip": "localhost", "port": port})
        return env

tune.register_env("CartPoleGRPCEnv", lambda c: EnvCreatorOnGRPC(30000)(c))
```

This env creator will run the `cartpole_grpc_server.py` script with the current PID as the parent PID and the port which is the function of `worker_index`. Then it will construct and return the client env, so that RLlib can start the sampling process.

Here is an example of how to run PPO on this environment with 64 workers (`rllib/examples/run_cartpole_with_grpc.py`)

```
    tune.register_env("CartPoleGRPCEnv", lambda c: EnvCreatorOnGRPC(30000)(c))
    config = (
        PPOConfig()
        .framework("torch")
        .environment("CartPoleGRPCEnv")
        .training(train_batch_size=8192)
        .rollouts(num_rollout_workers=64)
    )

    results = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=air.RunConfig(stop={"training_iteration": 1}),
    ).fit()
```


---
myst:
  html_meta:
    description: "Connect external simulators and applications to RLlib over the RLlink protocol, covering message types, client-server workflow, and episode exchange."
---

(rllib-external-env-setups-doc)=

# External environments and applications

Sometimes it doesn't make sense for RLlib to "step" an RL environment. For example, you might train a policy inside a complex simulator that runs its own execution loop, such as a game engine or a robotics simulation. A natural approach flips this setup around. Instead of RLlib stepping the environment, the agents in the simulation control their own stepping. An external, RLlib-powered service is available to answer queries for individual actions or to accept batched sample data. The service trains the policies but doesn't restrict when or how often per second the simulation steps.

```{figure} images/envs/external_env_setup_client_inference.svg
:align: left
:width: 600

**External application with client-side inference**: An external simulator, such as a game engine,
connects to RLlib, which runs as a server through a TCP-capable, custom EnvRunner.
The simulator periodically sends batches of data to the server and in turn receives weight updates.
For better performance, the client computes actions locally.
```

RLlib provides an [external messaging protocol](https://github.com/ray-project/ray/blob/master/rllib/env/external/rllink.py) called {ref}`RLlink <rllink-protocol-docs>` for this purpose. You can also customize your {py:class}`~ray.rllib.env.env_runner.EnvRunner` class to communicate through {ref}`RLlink <rllink-protocol-docs>` with one or more clients. An [example TCP-based EnvRunner implementation with RLlink](https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py) is available. It also contains a dummy CartPole client for testing and as a template for how your external application or simulator should use the {ref}`RLlink <rllink-protocol-docs>` protocol.

:::{note}
External application support is a work in progress on RLlib's new API stack. The Ray team is developing more examples for custom EnvRunner implementations, beyond [the available TCP-based one](https://github.com/ray-project/ray/blob/master/rllib/env/tcp_client_inference_env_runner.py), along with client-side, non-Python RLlib adapters for popular game engines and other simulation software.
:::

(rllink-protocol-docs)=

## The RLlink protocol

RLlink is a simple, stateful protocol for communication between a reinforcement learning (RL) server, such as RLlib, and an external client that acts as an environment simulator. It exchanges RL-specific data such as episodes, configuration, and model weights, and it supports on-policy training workflows.

### Key features

- **Stateful design**: The protocol maintains state across sequences of message exchanges, such as the request-response pair `GET_CONFIG` -> `SET_CONFIG`.
- **Strict request-response design**: Every exchange goes from a client request to a server response. Because the client simulation runs in its own execution loop, the server never sends unsolicited messages to clients.
- **RL-specific capabilities**: Tailored for RL workflows, including episode handling, model weight updates, and configuration management.
- **Flexible sampling**: Supports both on-policy and off-policy data collection modes.
- **JSON**: To simplify debugging and speed up iteration, the first versions of RLlink are entirely JSON-based, unencrypted, and insecure.

### Message structure

RLlink messages consist of a header and a body:

  - **Header**: An 8-byte length field giving the size of the body. For example, `00000016` indicates a body of length 16, and thus the total message size.
  - **Body**: JSON-encoded content with a `type` field indicating the message type.

#### Example messages: PING and EPISODES_AND_GET_STATE

Here is a complete example of the `PING` message. The 8-byte header encodes the size of the following body as length `16`, followed by the message body with the mandatory "type" field.

```
00000016{"type": "PING"}
```

The client sends the `PING` message after initiating a new connection. The server then responds with:

```
00000016{"type": "PONG"}
```

Here is an example `EPISODES_AND_GET_STATE` message that the client sends to the server, carrying a batch of sampling data. With the same message, the client asks the server to send back the updated model weights.

(example-rllink-episode-and-get-state-msg)=

```javascript
{
  "type": "EPISODES_AND_GET_STATE",
  "episodes": [
    {
      "obs": [[...]],  // List of observations
      "actions": [...],  // List of actions
      "rewards": [...],  // List of rewards
      "is_terminated": false,
      "is_truncated": false
    }
  ],
  "env_steps": 128
}
```

### Overview of all message types

#### Requests: Client → Server

- **`PING`**

  - Example: `{"type": "PING"}`.
  - Purpose: Initial handshake to establish communication.
  - Expected response: `{"type": "PONG"}`.

- **`GET_CONFIG`**

  - Example: `{"type": "GET_CONFIG"}`.
  - Purpose: Request the relevant configuration, such as how many timesteps to collect for a single `EPISODES_AND_GET_STATE` message. See below.
  - Expected response: `{"type": "SET_CONFIG", "env_steps_per_sample": 500, "force_on_policy": true}`.

- **`EPISODES_AND_GET_STATE`**

  - Example: {ref}`Example EPISODES_AND_GET_STATE message <example-rllink-episode-and-get-state-msg>`.
  - Purpose: Combine `EPISODES` and `GET_STATE` into a single request. This helps workflows that require on-policy, synchronous updates to model weights after data collection.
  - Body:

    - `episodes`: A list of JSON objects, each with the mandatory keys "obs" (list of observations in the episode), "actions" (list of actions in the episode), "rewards" (list of rewards in the episode), "is_terminated" (bool), and "is_truncated" (bool). The "obs" list has one more item than the "actions" and "rewards" lists because of the initial reset observation.
    - `weights_seq_no`: Sequence number for the model weights version, ensuring synchronization.

  - Expected response: `{"type": "SET_STATE", "weights_seq_no": 123, "mlir_file": ".. [b64 encoded string of the binary .mlir file with the model in it] .."}`.

#### Responses: Server → Client

- **`PONG`**

  - Example: `{"type": "PONG"}`.
  - Purpose: Acknowledgment of the `PING` request to confirm connectivity.

- **`SET_STATE`**

  - Example: `{"type": "SET_STATE", "weights_seq_no": 123, "onnx_file": "... [base64 encoded ONNX file] ..."}`.
  - Purpose: Provide the client with the current state, such as model weights.
  - Body:

    - `onnx_file`: Base64-encoded, compressed ONNX model file.
    - `weights_seq_no`: Sequence number for the model weights, ensuring synchronization.

- **`SET_CONFIG`**

  - Purpose: Send relevant configuration details to the client.
  - Body:

    - `env_steps_per_sample`: Number of total env steps collected for one `EPISODES_AND_GET_STATE` message.
    - `force_on_policy`: Whether to enforce on-policy sampling. If true, the client waits after sending the `EPISODES_AND_GET_STATE` message for the `SET_STATE` response before collecting the next round of samples.

#### Workflow examples

**Initial handshake**

1. Client sends `PING`.
1. Server responds with `PONG`.

**Configuration request**

1. Client sends `GET_CONFIG`.
1. Server responds with `SET_CONFIG`.

**On-policy training**

1. Client collects on-policy data and sends `EPISODES_AND_GET_STATE`.
1. Server processes the episodes and responds with `SET_STATE`.

:::{note}
This protocol is an initial draft toward a widely adopted protocol for communication between an external client and a remote RL service. Expect many changes, enhancements, and upgrades as it matures, including a safety layer and compression. It offers a lightweight, simple interface for integrating external environments with RL frameworks.
:::

## Example: External client connecting to a TCP-based EnvRunner

An [example TCP-based EnvRunner implementation with RLlink](https://github.com/ray-project/ray/blob/master/rllib/env/tcp_client_inference_env_runner.py) is available. See the [full end-to-end example](https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py).

You can alter the underlying logic of your custom EnvRunner. For example, you could implement a shared-memory communication layer instead of the TCP-based one.

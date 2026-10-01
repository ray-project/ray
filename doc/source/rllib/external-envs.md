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

RLlink is a simple, stateful protocol for communication between a reinforcement learning (RL) server, such as RLlib, and an external client that acts as an environment simulator. It exchanges RL-specific data such as episodes, configuration, and model weights, and it supports on-policy training workflows. The current protocol version is `0.0.1`.

### Key features

- **Stateful design**: The protocol maintains state across sequences of message exchanges, such as the request-response pair `GET_CONFIG` -> `SET_CONFIG`.
- **Client-initiated exchanges**: The client always initiates communication, and the server never sends an unsolicited message. The server replies only to requests that expect a response, such as `PING`, `GET_CONFIG`, `GET_STATE`, and `EPISODES_AND_GET_STATE`. A bare `EPISODES` message receives no reply.
- **RL-specific capabilities**: The protocol targets RL workflows, including episode handling, model weight updates, and configuration management.
- **Flexible sampling**: The protocol supports both on-policy data collection, through `EPISODES_AND_GET_STATE`, and off-policy collection, through `EPISODES`.
- **msgpack encoding**: RLlink encodes message bodies with [msgpack](https://msgpack.org/). The first versions of RLlink are unencrypted and insecure.

### Message structure

An RLlink message consists of a header and a body:

- **Header**: An 8-byte length field that holds the size of the body in bytes as an ASCII decimal number, left-padded with zeros. For example, `00000011` indicates a body of 11 bytes. The header isn't part of this count, so the total frame is 8 bytes plus the body length.
- **Body**: A msgpack-encoded dict with a mandatory `type` field that names the message type.

#### Example messages: PING and EPISODES_AND_GET_STATE

The `PING` message shows a complete frame. The body is the msgpack encoding of the dict `{"type": "PING"}`, which is 11 bytes long, so the header is `00000011`:

```text
b"00000011" + b"\x81\xa4type\xa4PING"
```

The client sends `PING` after it opens a new connection. The server responds with `PONG`, which frames the same way:

```text
b"00000011" + b"\x81\xa4type\xa4PONG"
```

The `EPISODES_AND_GET_STATE` message carries a batch of sampling data from the client to the server. With the same message, the client asks the server to send back the updated model weights. Each entry in `episodes` is a `SingleAgentEpisode.get_state()` dict, which the server reconstructs with `SingleAgentEpisode.from_state()`.

(example-rllink-episode-and-get-state-msg)=

```python
send_rllink_message(
    sock,
    {
        "type": "EPISODES_AND_GET_STATE",
        # One `SingleAgentEpisode.get_state()` dict per episode chunk.
        "episodes": [episode.get_state() for episode in episodes],
        "timesteps": 128,
    },
)
```

### Overview of all message types

#### Requests: Client → Server

- **`PING`**

  - Example: `{"type": "PING"}`.
  - Purpose: Initial handshake to establish communication.
  - Expected response: `PONG`.

- **`GET_CONFIG`**

  - Example: `{"type": "GET_CONFIG"}`.
  - Purpose: Request the algorithm configuration, which the client uses to build its local `RLModule` and to determine how many timesteps to collect before it sends an `EPISODES_AND_GET_STATE` message.
  - Expected response: `SET_CONFIG`.

- **`GET_STATE`**

  - Example: `{"type": "GET_STATE"}`.
  - Purpose: Request the current state, such as model weights, without sending any episodes.
  - Expected response: `SET_STATE`.

- **`EPISODES`**

  - Purpose: Send a batch of collected episodes to the server for off-policy training. The server ingests the episodes and sends no response.
  - Body: `episodes`, a list of `SingleAgentEpisode.get_state()` dicts.

- **`EPISODES_AND_GET_STATE`**

  - Example: {ref}`Example EPISODES_AND_GET_STATE message <example-rllink-episode-and-get-state-msg>`.
  - Purpose: Combine `EPISODES` and `GET_STATE` into a single request. This supports workflows that require on-policy, synchronous weight updates right after data collection.
  - Body:
    - `episodes`: A list of `SingleAgentEpisode.get_state()` dicts, one per episode chunk. The server reconstructs each episode with `SingleAgentEpisode.from_state()`.
    - `timesteps`: The number of environment steps in this batch.
  - Expected response: `SET_STATE`.

#### Responses: Server → Client

- **`PONG`**

  - Example: `{"type": "PONG"}`.
  - Purpose: Acknowledge a `PING` request and confirm connectivity.

- **`SET_CONFIG`**

  - Purpose: Send the algorithm configuration to the client.
  - Body:
    - `config`: A pickled `AlgorithmConfig`. The client deserializes it with `pickle.loads()` and builds its local `RLModule` from it. Because the payload is pickled, only connect a client to a server you trust.

- **`SET_STATE`**

  - Purpose: Provide the client with the current state, such as model weights.
  - Body:
    - `state`: A dict with two keys. `rl_module` holds the `RLModule` state, as returned by the module's `get_state()` method. `weights_seq_no` is the version number of the model weights. Comparing it across messages tells you how on-policy the collected data is, that is, whether the client sampled with the latest weights or an older set.

  Example shape:

  ```python
  {
      "type": "SET_STATE",
      "state": {
          "rl_module": ...,  # RLModule.get_state() output
          "weights_seq_no": 123,
      },
  }
  ```

#### Workflow examples

**Initial handshake**

1. Client sends `PING`.
1. Server responds with `PONG`.

**Configuration request**

1. Client sends `GET_CONFIG`.
1. Server responds with `SET_CONFIG`, and the client builds its local `RLModule` from the config.

**Initial weights request**

1. Client sends `GET_STATE`.
1. Server responds with `SET_STATE`, and the client loads the weights into its `RLModule`.

**On-policy training**

1. Client collects on-policy data and sends `EPISODES_AND_GET_STATE`.
1. Server ingests the episodes and responds with `SET_STATE`. The client blocks until it receives the response, then loads the updated weights before it collects the next batch.

:::{note}
This protocol is an initial draft toward a widely adopted protocol for communication between an external client and a remote RL service. Expect many changes, enhancements, and upgrades as it matures, including a safety layer and compression. It offers a lightweight, simple interface for integrating external environments with RL frameworks.
:::

## Example: External client connecting to a TCP-based EnvRunner

An [example TCP-based EnvRunner implementation with RLlink](https://github.com/ray-project/ray/blob/master/rllib/env/tcp_client_inference_env_runner.py) is available. See the [full end-to-end example](https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py).

You can alter the underlying logic of your custom EnvRunner. For example, you could implement a shared-memory communication layer instead of the TCP-based one.

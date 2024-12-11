.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-external-env-setups-doc:


External Environments and Applications
======================================

In many situations, it does not make sense for an RL environment to be "stepped" by RLlib.
For example, if you train one or more policies inside a complex simulator, for example a game engine
or a robotics simulation, it would be more natural and user friendly to flip this setup around
and - instead of RLlib "stepping" the env - allow the simulations and the agents to fully control
their own stepping. An external RLlib-powered service would be available for either querying for
individual actions or for accepting batched sample data. The service would cover the task
of training the policies, but would not pose any restrictions on when and how often
per second the simulation should step.

.. figure:: images/envs/external_env_setup_client_inference.svg
    :width: 600
    **External application with client-side inference**: An external simulator (for example a game engine)
    connects to RLlib, which runs as a server through a tcp-cabable, custom EnvRunner.
    The simulator sends batches of data from time to time to the server and in turn receives weights updates.
    For better performance, actions are computed locally on the client side.

.. todo (sven): show new image here with UE5
.. .. figure:: images/rllib-training-inside-a-unity3d-env.png
.. scale: 75 %
..    A Unity3D soccer game being learnt by RLlib via the ExternalEnv API.

RLlib provides an `external messaging protocol <https://github.com/ray-project/ray/blob/master/rllib/env/utils/external_env_protocol.py>`__
called :ref:`RLlink <rllink-protocol-docs>` for this purpose as well as the option to customize your :py:class:`~ray.rllib.env.env_runner.EnvRunner` class
toward communicating through :ref:`RLlink <rllink-protocol-docs>` with one or more clients.
An example, `tcp-based EnvRunner implementation with RLlink is available here <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py>`__.
It also contains a dummy (CartPole) client that can be used for testing and as a template for how your external application or simulator should
utilize the :ref:`RLlink <rllink-protocol-docs>` protocol.

.. note::
    External application support is still work-in-progress on RLlib's new API stack. The Ray team
    is working on more examples for custom EnvRunner implementations (besides
    `the already available tcp-based one <https://github.com/ray-project/ray/blob/master/rllib/env/tcp_client_inference_env_runner.py>`__)
    as well as various client-side, non-python RLlib-adapters, for example for popular game engines and other
    simulation software.


.. _rllink-protocol-docs:

The RLlink Protocol
-------------------

RLLink is a simple, stateful protocol designed for communication between a reinforcement learning (RL) server (ex., RLlib) and an
external client acting as an environment simulator. The protocol enables seamless exchange of RL-specific data such as episodes,
configuration, and model weights, while also facilitating on-policy training workflows.

Key Features
~~~~~~~~~~~~

- **Stateful Design**: The protocol maintains some state through sequences of message exchanges (ex., request-response pairs like `GET_CONFIG` -> `SET_CONFIG`).
- **Strict Request-Response Design**: The protocol is strictly (client) request -> (server) response based. Due to the necessity to let the client simulation run in its own execution loop, the server side refrains from sending any unsolicited messages to the clients.
- **RL-Specific Capabilities**: Tailored for RL workflows, including episode handling, model weight updates, and configuration management.
- **Flexible Sampling**: Supports both on-policy and off-policy data collection modes.
- **JSON**: For reasons of better debuggability and faster iterations, the first versions of RLlink are entirely JSON-based, non-encrypted, and non-secure.

Message Structure
~~~~~~~~~~~~~~~~~

RLlink messages consist of a header and a body:

  - **Header**: 8-byte length field indicating the size of the body, for example `00000016` for a body of length 16 (thus, in total, the message size ).
  - **Body**: JSON-encoded content with a `type` field indicating the message type.

Example Messages: PING and EPISODES_AND_GET_STATE
+++++++++++++++++++++++++++++++++++++++++++++++++

Here is a complete simple example message for the `PING` message. Note the 8-byte header
encoding the size of the following body to be of length `16`, followed by the message body with the mandatory "type" field.

.. code-block::

    00000016{"type": "PING"}


The `PING` message should be sent by the client after initiation of a new connection. The server
then responds with:

.. code-block::

    00000016{"type": "PONG"}


Here is an example of a `EPISODES_AND_GET_STATE` message sent by the client to the server and carrying
a batch of sampling data. Also, the client asks the server to send back the updated model weights.

.. code-block:: json

    {
      "type": "EPISODES_AND_GET_STATE",
      "episodes": [
        {
          "obs": [[...]],  // List of observations
          "actions": [...],  // List of actions
          "rewards": [...],  // List of rewards
          "terminated": false,
          "truncated": false
        }
      ],
      "timesteps": 128
    }


Message Types
~~~~~~~~~~~~~



Requests: Client → Server
+++++++++++++++++++++++++
- **`PING`**
  - Purpose: Initial handshake to establish communication.
  - Expected Response: `PONG`.

- **`EPISODES`**
  - Purpose: Send a batch of completed episodes to the server for processing.
  - Body:
    - `episodes`: List of serialized episode data.

- **`GET_STATE`**
  - Purpose: Request the current state, typically model weights, from the server.
  - Expected Response: `SET_STATE`.

- **`GET_CONFIG`**
  - Purpose: Request the relevant configuration (e.g., rollout fragment length).
  - Expected Response: `SET_CONFIG`.

- **`EPISODES_AND_GET_STATE`**
  - Purpose: Combine `EPISODES` and `GET_STATE` into a single request. This is useful for workflows requiring synchronous updates to model weights after data collection.
  - Expected Response: `SET_STATE`.

---

#### **2. Responses: Server → Client**
- **`PONG`**
  - Purpose: Acknowledgment of the `PING` request to confirm connectivity.

- **`SET_STATE`**
  - Purpose: Provide the client with the current state (e.g., model weights).
  - Body:
    - `onnx_file`: Base64-encoded, compressed ONNX model file.
    - `weights_seq_no`: Sequence number for the model weights, ensuring synchronization.

- **`SET_CONFIG`**
  - Purpose: Send relevant configuration details to the client.
  - Body:
    - `env_steps_per_sample`: Number of steps per sampling batch.
    - `force_on_policy`: Whether on-policy sampling is enforced.

---

### Workflow Examples

#### **Initial Handshake**
1. Client sends `PING`.
2. Server responds with `PONG`.

#### **On-Policy Training**
1. Client collects on-policy data and sends `EPISODES_AND_GET_STATE`.
2. Server processes the episodes and responds with `SET_STATE`.

#### **Configuration Request**
1. Client sends `GET_CONFIG`.
2. Server responds with `SET_CONFIG`.

---


---

### Security Considerations
- Use **TLS over TCP** to encrypt data and prevent eavesdropping.
- Authenticate connections using pre-shared keys or certificates.
- Validate message contents to prevent injection attacks.

---

### Implementation Notes
- The `TcpClientInferenceEnvRunner` manages communication and ensures state consistency.
- The server socket listens for a single client connection at a time and processes messages in a loop.
- A dummy client implementation demonstrates the protocol in a test scenario, using the `CartPole` environment for sample data.

---

This protocol offers a lightweight yet powerful interface for integrating external environments with RL frameworks, emphasizing simplicity, extensibility, and compatibility with on-policy and off-policy workflows.





Example: External client connecting to tcp-based EnvRunner
----------------------------------------------------------

Let's walk through an end-to-end setup to demonstrate how to use RLlib's external env APIs
and messaging protocols. You are going to implement a simple, custom :py:class:`~ray.rllib.env.env_runner.EnvRunner`
acting as the RL server, accepting tcp client connections and exchanging messages.

Batches of experiences are sent from the client to your `EnvRunner`, which then sends back updated model weights.

`See here for a complete implementation of such an EnvRunner <https://github.com/ray-project/ray/blob/master/rllib/env/tcp_client_inference_env_runner.py>`__.




.. testcode::


    # Define the RLlib (server) config.
    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            observation_space=gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
            action_space=gym.spaces.Discrete(2),
            # EnvRunners listen on `port` + their worker index.
            env_config={"port": args.port},
        )
        .env_runners(
            # Point RLlib to the custom EnvRunner to be used here.
            env_runner_cls=TcpClientInferenceEnvRunner,
        )
        .training(
            num_epochs=10,
            vf_loss_coeff=0.01,
        )
        .rl_module(model_config=DefaultModelConfig(vf_share_layers=True))
    )




.. code-block:: python

    config = {
        # An environment class is still required, but it doesn't need to be runnable.
        # You only need to define its action and observation space attributes.
        # See examples/envs/external_envs/unity3d_server.py for an example using a RandomMultiAgentEnv stub.
        "env": YOUR_ENV_STUB,
        # Use the policy server to generate experiences.
        "input": (
            lambda ioctx: PolicyServerInput(ioctx, SERVER_ADDRESS, SERVER_PORT)
        ),
        # Use the existing algorithm process to run the server.
        "num_env_runners": 0,
    }



See these examples for a `simple "CartPole-v1" server <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/cartpole_server.py>`__
and `n client(s) <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/cartpole_client.py>`__
scripts, in which we setup an RLlib policy server that listens on one or more ports for
client connections and connect several clients to this server to learn the env.



Clients can then connect in either *local* or *remote* inference mode.
In local inference mode, copies of the policy are downloaded from the server and cached on the client for a configurable period of time.
This allows actions to be computed by the client without requiring a network round trip each time.
In remote inference mode, each computed action requires a network call to the server.

Example:

.. code-block:: python

    client = PolicyClient("http://localhost:9900", inference_mode="local")
    episode_id = client.start_episode()
    ...
    action = client.get_action(episode_id, cur_obs)
    ...
    client.end_episode(episode_id, last_obs)

To understand the difference between standard envs, external envs, and connecting with a ``PolicyClient``, refer to the following figure:

.. https://docs.google.com/drawings/d/1hJvT9bVGHVrGTbnCZK29BYQIcYNRbZ4Dr6FOPMJDjUs/edit
.. image:: images/rllib-external.svg

Try it yourself by launching either a
`simple CartPole server <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/cartpole_server.py>`__ (see below), and connecting it to any number of clients
(`cartpole_client.py <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/cartpole_client.py>`__) or
run a `Unity3D learning sever <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/unity3d_server.py>`__
against distributed Unity game engines in the cloud.

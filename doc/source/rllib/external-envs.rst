.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-external-env-setups-doc:


External Environments and Applications
--------------------------------------

In many situations, it doesn't make sense for an RL environment to be "stepped" by RLlib.
For example, if you train one or more policies inside a complex simulator, like a game engine
or a robotics simulation. A natural and user friendly approach is to flip this setup around
and - instead of RLlib "stepping" the env - allow the simulations and the agents to fully control
their own stepping. An external RLlib-powered service would be available for either querying for
individual actions or for accepting batched sample data. The service would cover the task
of training the policies, but wouldn't pose any restrictions on when and how often
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
for this purpose as well as the option to customize your :py:class:`~ray.rllib.env.env_runner.EnvRunner` class used for collecting
training data. An example, `tcp-based EnvRunner implementation is available here <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py>`__,
together with a dummy (CartPole) client providing a template for any external application or simulator.

.. note::
    External application support is still work-in-progress on RLlib's new API stack. The Ray team
    is working on more examples for custom EnvRunner implementations (besides
    `the already available tcp-based one <https://github.com/ray-project/ray/blob/master/rllib/env/tcp_client_inference_env_runner.py>`__)
    as well as client-side, non-python RLlib-adapters, for example for popular game engines and other
    simulation software.






At any point, agents on that thread can query the current policy for decisions with
``self.get_action()`` and reports rewards, done-dicts, and infos with ``self.log_returns()``.
You can do this query for multiple concurrent episodes as well.

See these examples for a `simple "CartPole-v1" server <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/cartpole_server.py>`__
and `n client(s) <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/cartpole_client.py>`__
scripts, which sets up an RLlib policy server that listens on one or more ports for
client connections and connect several clients to this server to learn the env.


External Application Clients
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For applications that are running entirely outside the Ray cluster (i.e., cannot be
packaged into a Python environment of any form), RLlib provides the ``PolicyServerInput``
application connector, which can be connected to over the network using ``PolicyClient``
instances.

You can configure any Algorithm to launch a policy server with the following config:

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

CartPole Example:

.. code-block:: bash

    # Start the server by running:
    >>> python rllib/examples/envs/external_envs/cartpole_server.py --run=PPO
    --
    -- Starting policy server at localhost:9900
    --

    # To connect from a client with inference_mode="remote".
    >>> python rllib/examples/envs/external_envs/cartpole_client.py --inference-mode=remote
    Total reward: 10.0
    Total reward: 58.0
    ...
    Total reward: 200.0
    ...

    # To connect from a client with inference_mode="local" (faster).
    >>> python rllib/examples/envs/external_envs/cartpole_client.py --inference-mode=local
    Querying server for new policy weights.
    Generating new batch of experiences.
    Total reward: 13.0
    Total reward: 11.0
    ...
    Sending batch of 1000 steps back to server.
    Querying server for new policy weights.
    ...
    Total reward: 200.0
    ...

For the best performance, we recommend using ``inference_mode="local"`` when possible.

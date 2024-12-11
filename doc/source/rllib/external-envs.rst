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
for this purpose as well as the option to customize your :py:class:`~ray.rllib.env.env_runner.EnvRunner` class used for collecting
training data. An example, `tcp-based EnvRunner implementation is available here <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py>`__,
together with a dummy (CartPole) client providing a template for any external application or simulator.

.. note::
    External application support is still work-in-progress on RLlib's new API stack. The Ray team
    is working on more examples for custom EnvRunner implementations (besides
    `the already available tcp-based one <https://github.com/ray-project/ray/blob/master/rllib/env/tcp_client_inference_env_runner.py>`__)
    as well as various client-side, non-python RLlib-adapters, for example for popular game engines and other
    simulation software.


Example: External client connecting to tcp-based EnvRunner
----------------------------------------------------------

Let's walk through an end-to-end setup to demonstrate how to use RLlib's external env APIs
and messaging protocols. You are going to implement a simple, custom :py:class:`~ray.rllib.env.env_runner.EnvRunner`
capable of accepting a tcp client connection and exchanging messages with



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

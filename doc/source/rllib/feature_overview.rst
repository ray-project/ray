.. List of most important features of RLlib, with sigil-like buttons for each of the features.
    To be included into different rst files.

.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1yEOfeHvuLi5EzZKtGFQMfQ2NINzi3bUBrU3Z7bCiuKs/edit

        .. image:: images/sigils/rllib-sigil-distributed-learning.svg
            :width: 100
            :target: https://github.com/ray-project/ray/blob/master/rllib/utils/framework.py

    .. container::

        **Highly distributed learning**: Our RLlib algorithms (such as our "PPO" or "IMPALA")
        allow you to set the ``num_env_runners`` config parameter, such that your workloads can run
        on 100s of CPUs/nodes thus parallelizing and speeding up learning.


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1Lbi1Zf5SvczSliGEWuK4mjWeehPIArYY9XKys81EtHU/edit

        .. image:: images/sigils/rllib-sigil-multi-agent.svg
            :width: 100
            :target: https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_independent_learning.py

    .. container::

        | **Multi-agent RL** (MARL): Convert your (custom) ``gym.Envs`` into a multi-agent one
          via a few simple steps and start training your agents in any of the following fashions:
        | 1) Cooperative with `shared <https://github.com/ray-project/ray/blob/master/rllib/examples/centralized_critic.py>`_ or
          `separate <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/two_step_game_with_grouped_agents.py>`_
          policies and/or value functions.
        | 2) Adversarial scenarios using `self-play <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/self_play_with_open_spiel.py>`_
          and `league-based training <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/self_play_league_based_with_open_spiel.py>`_.
        | 3) `Independent learning <https://github.com/ray-project/ray/blob/master/rllib/examples/multi_agent/pettingzoo_independent_learning.py>`_
          of neutral/co-existing agents.


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1DY2IJUPo007mSRylz6IEs-dz_n1-rFh67RMi9PB2niY/edit

        .. image:: images/sigils/rllib-sigil-external-simulators.svg
            :width: 100
            :target: https://github.com/ray-project/ray/tree/master/rllib/examples/envs/external_envs

    .. container::

        **External simulators**: Don't have your simulation running as a gym.Env in python?
        No problem! RLlib supports an external environment API and comes with a pluggable,
        off-the-shelve
        `client <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/cartpole_client.py>`_/
        `server <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/external_envs/cartpole_server.py>`_
        setup that allows you to run 100s of independent simulators on the "outside"
        (e.g. a Windows cloud) connecting to a central RLlib Policy-Server that learns
        and serves actions. Alternatively, actions can be computed on the client side
        to save on network traffic.


.. container:: clear-both

    .. container:: buttons-float-left

        .. https://docs.google.com/drawings/d/1VFuESSI5u9AK9zqe9zKSJIGX8taadijP7Qw1OLv2hSQ/edit

        .. image:: images/sigils/rllib-sigil-offline-rl.svg
            :width: 100
            :target: https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl/offline_rl.py

    .. container::

        **Offline RL and imitation learning/behavior cloning**: You don't have a simulator
        for your particular problem, but tons of historic data recorded by a legacy (maybe
        non-RL/ML) system? This branch of reinforcement learning is for you!
        RLlib's comes with several `offline RL <https://github.com/ray-project/ray/blob/master/rllib/examples/offline_rl/offline_rl.py>`_
        algorithms (*CQL*, *MARWIL*, and *DQfD*), allowing you to either purely
        `behavior-clone <https://github.com/ray-project/ray/blob/master/rllib/algorithms/bc/tests/test_bc.py>`_
        your existing system or learn how to further improve over it.

Policy Gradient Methods
=======================

This code shows how to do reinforcement learning with policy gradient methods.
View the `code for this example`_.

.. note::

    For an overview of Ray's reinforcement learning library, see `RLlib <http://ray.readthedocs.io/en/latest/rllib.html>`__.


To run this example, you will need to install `TensorFlow with GPU support`_ (at
least version ``1.0.0``) and a few other dependencies.

.. code-block:: bash

  pip install gym[atari]
  pip install tensorflow

Then you can run the example as follows.

.. code-block:: bash

  rllib train --env=Pong-ram-v4 --run=PPO

This will train an agent on the ``Pong-ram-v4`` Atari environment. You can also
try passing in the ``Pong-v0`` environment or the ``CartPole-v0`` environment.
If you wish to use a different environment, you will need to change a few lines
in ``example.py``.

Current and historical training progress can be monitored by pointing
TensorBoard to the log output directory as follows.

.. code-block:: bash

  tensorboard --logdir=~/ray_results

Many of the TensorBoard metrics are also printed to the console, but you might
find it easier to visualize and compare between runs using the TensorBoard UI.

.. _`TensorFlow with GPU support`: https://www.tensorflow.org/install/
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/rllib/agents/ppo

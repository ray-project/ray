Policy Gradient Methods
=======================

This code shows how to do reinforcement learning with policy gradient methods.
View the `code for this example`_.

To run this example, you will need to install `TensorFlow with GPU support`_ (at
least version ``1.0.0``) and a few other dependencies.

.. code-block:: bash

  pip install gym[atari]
  pip install tensorflow

Then install the package as follows.

.. code-block:: bash

  cd ray/examples/policy_gradient/
  python setup.py install

Then you can run the example as follows.

.. code-block:: bash

  python ray/examples/policy_gradient/examples/example.py --environment=Pong-ram-v3

This will train an agent on the ``Pong-ram-v3`` Atari environment. You can also
try passing in the ``Pong-v0`` environment or the ``CartPole-v0`` environment.
If you wish to use a different environment, you will need to change a few lines
in ``example.py``.

Current and historical training progress can be monitored by pointing
TensorBoard to the log output directory as follows.

.. code-block:: bash

  tensorboard --logdir=/tmp/ray

Many of the TensorBoard metrics are also printed to the console, but you might
find it easier to visualize and compare between runs using the TensorBoard UI.

.. _`TensorFlow with GPU support`: https://www.tensorflow.org/install/
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/policy_gradient

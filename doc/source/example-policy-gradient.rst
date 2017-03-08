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

  python ray/examples/policy_gradient/examples/example.py

This will train an agent on an Atari environment.

.. _`TensorFlow with GPU support`: https://www.tensorflow.org/install/
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/policy_gradient

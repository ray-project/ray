Asynchronous Advantage Actor Critic (A3C)
===========================

This document provides a walkthrough of an implementation of  `A3C`_, 
a state-of-the-art reinforcement learning algorithm. 

.. _`A3C`: https://arxiv.org/abs/1602.01783

Note that this is a modified version of OpenAI's `Universe Starter Agent`_.
The main modifications to the original code are the usage of Ray to start 
new processes as opposed to starting subprocesses and coordinating with the
TensorFlow server and the removal of the ``universe`` dependency. 

.. _`Universe Starter Agent`: https://github.com/openai/universe-starter-agent 

To run the application, first install some dependencies.

.. code-block:: bash

  pip install tensorflow
  pip install six
  pip install gym[atari]
  conda install -y -c https://conda.binstar.org/menpo opencv3
  conda install -y numpy
  conda install -y scipy

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/richardliaw/ray/tree/master/examples/a3c

Reinforcement Learning
----------------------

Reinforcement Learning is an area of machine learning concerned with how an agent should act
in an environment, directing efforts to maximize some form of cumulative reward. 
Typically, an agent will take in an observation and take an action given its observation. 
The action will change the state of the environment and will provide some numerical reward 
(or penalty) to the agent. The agent will then take in another observation as a result 
and take another action. The mapping from state to action is a policy, and in deep reinforcement
learning, this policy is often represented with a deep neural network.

Simulations are often used as the environment, effectively allowing different processes
to each have their own instance of simulation and allow for a larger stream of experience.

In order to improve the policy, a gradient is taken with respect to the actions and states
observed. In our A3C implementation, each worker, implemented as an actor object,
is continuously simulating the environment. The driver will trigger a gradient calculation
on the worker, which the worker will then send back to the driver. The driver will then add
the gradients and then trigger the gradient calculation again. 

There are two main parts to the implementation - the driver and the worker.


Driver Code Walkthrough
-----------------------

.. code-block:: python

  import numpy as np
  import ray

  def train(num_workers, env_name="PongDeterministic-v3"):
    env = create_env(env_name, None, None)
    policy = LSTMPolicy(env.observation_space.shape, env.action_space.n, 0)
    agents = [Runner(env_name, i) for i in range(num_workers)]
    parameters = policy.get_weights()
    gradient_list = [agent.compute_gradient(parameters) for agent in agents]
    steps = 0
    obs = 0
    while True:
      done_id, gradient_list = ray.wait(gradient_list)
      gradient, info = ray.get(done_id)[0]
      policy.model_update(gradient)
      parameters = policy.get_weights()
      steps += 1
      obs += info["size"]
      gradient_list.extend([agents[info["id"]].compute_gradient(parameters)])
    return policy

Basic random search
-------------------

Something that works surprisingly well is to try random values for the
hyperparameters. For example, we can write a function that randomly generates
hyperparameter configurations.

.. code-block:: python

  def generate_hyperparameters():
    # Randomly choose values for the hyperparameters.
    return {"learning_rate": 10 ** np.random.uniform(-5, 5),
            "batch_size": np.random.randint(1, 100),
            "dropout": np.random.uniform(0, 1),
            "stddev": 10 ** np.random.uniform(-5, 5)}

In addition, let's assume that we've started Ray and loaded some data.

.. code-block:: python

  import ray

  ray.init()

  from tensorflow.examples.tutorials.mnist import input_data
  mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
  train_images = ray.put(mnist.train.images)
  train_labels = ray.put(mnist.train.labels)
  validation_images = ray.put(mnist.validation.images)
  validation_labels = ray.put(mnist.validation.labels)


Then basic random hyperparameter search looks something like this. We launch a
bunch of experiments, and we get the results.

.. code-block:: python

  # Generate a bunch of hyperparameter configurations.
  hyperparameter_configurations = [generate_hyperparameters() for _ in range(20)]

  # Launch some experiments.
  results = []
  for hyperparameters in hyperparameter_configurations:
    results.append(train_cnn_and_compute_accuracy.remote(hyperparameters,
                                                         train_images,
                                                         train_labels,
                                                         validation_images,
                                                         validation_labels))

  # Get the results.
  accuracies = ray.get(results)

Then we can inspect the contents of `accuracies` and see which set of
hyperparameters worked the best. Note that in the above example, the for loop
will run instantaneously and the program will block in the call to ``ray.get``,
which will wait until all of the experiments have finished.

Deviations from the original A3C implementation
-----------------------------------------------

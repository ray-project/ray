Asynchronous Advantage Actor Critic (A3C)
=========================================

This document walks through `A3C`_, a state-of-the-art reinforcement learning
algorithm. In this example, we adapt the OpenAI `Universe Starter Agent`_
implementation of A3C to use Ray.

View the `code for this example`_.

.. _`A3C`: https://arxiv.org/abs/1602.01783
.. _`Universe Starter Agent`: https://github.com/openai/universe-starter-agent
.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/a3c

To run the application, first install **ray** and then some dependencies:

.. code-block:: bash

  pip install tensorflow
  pip install six
  pip install gym[atari]==0.7.4
  pip install opencv-python
  pip install scipy

Note that this code **currently does not work** with ``gym==0.8.0``.

You can run the code with

.. code-block:: bash

  python ray/examples/a3c/driver.py [num_workers]

Reinforcement Learning
----------------------

Reinforcement Learning is an area of machine learning concerned with **learning
how an agent should act in an environment** so as to maximize some form of
cumulative reward. Typically, an agent will observe the current state of the
environment and take an action based on its observation. The action will change
the state of the environment and will provide some numerical reward (or penalty)
to the agent. The agent will then take in another observation and the process
will repeat. **The mapping from state to action is a policy**, and in
reinforcement learning, this policy is often represented with a deep neural
network.

The **environment** is often a simulator (for example, a physics engine), and
reinforcement learning algorithms often involve trying out many different
sequences of actions within these simulators. These **rollouts** can often be
done in parallel.

Policies are often initialized randomly and incrementally improved via
simulation within the environment. To improve a policy, gradient-based updates
may be computed based on the sequences of states and actions that have been
observed. The gradient calculation is often delayed until a termination
condition is reached (that is, the simulation has finished) so that delayed
rewards have been properly accounted for. However, in the Actor Critic model, we
can begin the gradient calculation at any point in the simulation rollout by
predicting future rewards with a Value Function approximator.

In our A3C implementation, each worker, implemented as a Ray actor, continuously
simulates the environment. The driver will create a task that runs some steps
of the simulator using the latest model, computes a gradient update, and returns
the update to the driver. Whenever a task finishes, the driver will use the
gradient update to update the model and will launch a new task with the latest
model.

There are two main parts to the implementation - the driver and the worker.

Worker Code Walkthrough
-----------------------

We use a Ray Actor to simulate the environment.

.. code-block:: python

  import numpy as np
  import ray

  @ray.actor
  class Runner(object):
    """Actor object to start running simulation on workers.
        Gradient computation is also executed on this object."""
    def __init__(self, env_name, actor_id):
      # starts simulation environment, policy, and thread.
      # Thread will continuously interact with the simulation environment
      self.env = env = create_env(env_name)
      self.id = actor_id
      self.policy = LSTMPolicy()
      self.runner = RunnerThread(env, self.policy, 20)
      self.start()

    def start(self):
      # starts the simulation thread
      self.runner.start_runner()

    def pull_batch_from_queue(self):
      # Implementation details removed - gets partial rollout from queue
      return rollout

    def compute_gradient(self, params):
      self.policy.set_weights(params)
      rollout = self.pull_batch_from_queue()
      batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
      gradient = self.policy.get_gradients(batch)
      info = {"id": self.id,
              "size": len(batch.a)}
      return gradient, info

Driver Code Walkthrough
-----------------------

The driver manages the coordination among workers and handles updating the
global model parameters. The main training script looks like the following.


.. code-block:: python

  import numpy as np
  import ray

  def train(num_workers, env_name="PongDeterministic-v3"):
    # Setup a copy of the environment
    # Instantiate a copy of the policy - mainly used as a placeholder
    env = create_env(env_name, None, None)
    policy = LSTMPolicy(env.observation_space.shape, env.action_space.n, 0)
    obs = 0

    # Start simulations on actors
    agents = [Runner(env_name, i) for i in range(num_workers)]

    # Start gradient calculation tasks on each actor
    parameters = policy.get_weights()
    gradient_list = [agent.compute_gradient(parameters) for agent in agents]

    while True: # Replace with your termination condition
      # wait for some gradient to be computed - unblock as soon as the earliest arrives
      done_id, gradient_list = ray.wait(gradient_list)

      # get the results of the task from the object store
      gradient, info = ray.get(done_id)[0]
      obs += info["size"]

      # apply update, get the weights from the model, start a new task on the same actor object
      policy.model_update(gradient)
      parameters = policy.get_weights()
      gradient_list.extend([agents[info["id"]].compute_gradient(parameters)])
    return policy


Benchmarks and Visualization
----------------------------

For the `PongDeterministic-v3` and an Amazon EC2 m4.16xlarge instance, we are able to train the agent with 16 workers in just under 15 minutes. With 8 workers, we can train the agent in just under 25 minutes.

You can visualize performance by running `tensorboard --logdir [directory]` in a separate screen, where `[directory]` is defaulted to `./tmp/`.

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
observed. Generally, the gradient calculation must be delayed until a termination condition
is reached (completing a rollout) so that delayed rewards for an action is properly 
allocated. However, in the Actor Critic model, we can begin the gradient calculation at any point 
in the simulation rollout by predicting future rewards with a Value Function approximator 
(read more here).

In our A3C implementation, each worker, implemented as an actor object,
is continuously simulating the environment. The driver will trigger a gradient calculation
on the worker, which the worker will then send back to the driver. The driver will then add
the gradients and then trigger the gradient calculation again. 

There are two main parts to the implementation - the driver and the worker.

Worker Code Walkthrough
-------------------
We use an Actor object to implement the simulation. 

.. code-block:: python

  import numpy as np
  import ray

  @ray.actor
  class Runner(object):
    """Actor object to start running simulation on workers. 
        Gradient computation is also executed on this object."""
    def __init__(self, env_name, actor_id, logdir="tmp/"):
      # starts simulation environment, policy, and thread.
      # Thread will continuously interact with the simulation environment
      self.env = env = create_env(env_name)
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
              "size": len(batch.a) }
      return gradient, info
      


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
    
Deviations from the original A3C implementation
-----------------------------------------------

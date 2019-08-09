"""
Learning to Play Pong
=====================

In this example, we'll train a **very simple** neural network to play Pong using
the OpenAI Gym. This application is adapted, with minimal modifications, from
Andrej Karpathy's `source code`_ (see the accompanying `blog post`_).

To run the application, first install some dependencies.

.. code-block:: bash

  pip install gym[atari]

To run the example on a cluster, simply pass in the flag
``--redis-address=<redis-address>``.

At the moment, on a large machine with 64 physical cores, computing an update
with a batch of size 1 takes about 1 second, a batch of size 10 takes about 2.5
seconds. A batch of size 60 takes about 3 seconds. On a cluster with 11 nodes,
each with 18 physical cores, a batch of size 300 takes about 10 seconds. If the
numbers you see differ from these by much, take a look at the
**Troubleshooting** section at the bottom of this page and consider `submitting
an issue`_.

.. _`source code`: https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5
.. _`blog post`: http://karpathy.github.io/2016/05/31/rl/
.. _`submitting an issue`: https://github.com/ray-project/ray/issues

**Note** that these times depend on how long the rollouts take, which in turn
depends on how well the policy is doing. For example, a really bad policy will
lose very quickly. As the policy learns, we should expect these numbers to
increase.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import numpy as np
import os
import ray
import time

import gym

##############################################################################
# Hyperparameters
# ---------------

# Here we'll define a couple of the hyperparameters that are used.


# The number of hidden layer neurons.
H = 200
learning_rate = 1e-4
# Discount factor for reward.
gamma = 0.99
# The decay factor for RMSProp leaky sum of grad^2.
decay_rate = 0.99
# The input dimensionality: 80x80 grid.
D = 80 * 80

##############################################################################
# Sigmoid is used to "squash" the output to be between 0 and 1.

def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))

#############################################################################
# Preprocessing
# -------------

# Preprocessing can greatly affect the learning performance. The below code will
# preprocess the original 210x160x3 uint8 frame into a one-dimensional 6400
# float vector.


def preprocess(img):
    # Crop the image.
    img = img[35:195]
    # Downsample by factor of 2.
    img = img[::2, ::2, 0]
    # Erase background (background type 1).
    img[img == 144] = 0
    # Erase background (background type 2).
    img[img == 109] = 0
    # Set everything else (paddles, ball) to 1.
    img[img != 0] = 1
    return img.astype(np.float).ravel()


def discount_rewards(r):
    """take 1D float array of rewards and compute discounted reward"""
    discounted_r = np.zeros_like(r)
    running_add = 0
    for t in reversed(range(0, r.size)):
        # Reset the sum, since this was a game boundary (pong specific!).
        if r[t] != 0:
            running_add = 0
        running_add = running_add * gamma + r[t]
        discounted_r[t] = running_add
    return discounted_r


def policy_forward(x, model):
    h = np.dot(model["W1"], x)
    h[h < 0] = 0  # ReLU nonlinearity.
    logp = np.dot(model["W2"], h)
    p = sigmoid(logp)
    # Return probability of taking action 2, and hidden state.
    return p, h


def policy_backward(eph, epx, epdlogp, model):
    """backward pass. (eph is array of intermediate hidden states)"""
    dW2 = np.dot(eph.T, epdlogp).ravel()
    dh = np.outer(epdlogp, model["W2"])
    # Backprop relu.
    dh[eph <= 0] = 0
    dW1 = np.dot(dh.T, epx)
    return {"W1": dW1, "W2": dW2}


#############################################################################
# Here, a neural network is used to define a "policy"
# for playing Pong (that is, a function that chooses an action given a state). In
# the loop, the network repeatedly plays games of Pong and records a gradient from
# each game. Every ten games, the gradients are combined together and used to
# update the network.
# This example is easy to parallelize because the network can play ten games in
# parallel and no information needs to be shared between the games.
# We define an **actor** for the Pong environment, which includes a method for
# performing a rollout and computing a gradient update. Below is pseudocode for
# the actor.


@ray.remote
class PongEnv(object):
    def __init__(self):
        # Tell numpy to only use one core. If we don't do this, each actor may
        # try to use all of the cores and the resulting contention may result
        # in no speedup over the serial version. Note that if numpy is using
        # OpenBLAS, then you need to set OPENBLAS_NUM_THREADS=1, and you
        # probably need to do it from the command line (so it happens before
        # numpy is imported).
        os.environ["MKL_NUM_THREADS"] = "1"
        self.env = gym.make("Pong-v0")

    def compute_gradient(self, model):
        # Reset the game.
        observation = self.env.reset()
        # Note that prev_x is used in computing the difference frame.
        prev_x = None
        xs, hs, dlogps, drs = [], [], [], []
        reward_sum = 0
        done = False
        while not done:
            cur_x = preprocess(observation)
            x = cur_x - prev_x if prev_x is not None else np.zeros(D)
            prev_x = cur_x

            aprob, h = policy_forward(x, model)
            # Sample an action.
            action = 2 if np.random.uniform() < aprob else 3

            # The observation.
            xs.append(x)
            # The hidden state.
            hs.append(h)
            y = 1 if action == 2 else 0  # A "fake label".
            # The gradient that encourages the action that was taken to be
            # taken (see http://cs231n.github.io/neural-networks-2/#losses if
            # confused).
            dlogps.append(y - aprob)

            observation, reward, done, info = self.env.step(action)
            reward_sum += reward

            # Record reward (has to be done after we call step() to get reward
            # for previous action).
            drs.append(reward)

        epx = np.vstack(xs)
        eph = np.vstack(hs)
        epdlogp = np.vstack(dlogps)
        epr = np.vstack(drs)
        # Reset the array memory.
        xs, hs, dlogps, drs = [], [], [], []

        # Compute the discounted reward backward through time.
        discounted_epr = discount_rewards(epr)
        # Standardize the rewards to be unit normal (helps control the gradient
        # estimator variance).
        discounted_epr -= np.mean(discounted_epr)
        discounted_epr /= np.std(discounted_epr)
        # Modulate the gradient with advantage (the policy gradient magic
        # happens right here).
        epdlogp *= discounted_epr
        return policy_backward(eph, epx, epdlogp, model), reward_sum


iterations = 20
batch_size = 10

ray.init()

#############################################################################
# Run the reinforcement learning.

running_reward = None
batch_num = 1
model = {}
# "Xavier" initialization.
model["W1"] = np.random.randn(H, D) / np.sqrt(D)
model["W2"] = np.random.randn(H) / np.sqrt(H)
# Update buffers that add up gradients over a batch.
grad_buffer = {k: np.zeros_like(v) for k, v in model.items()}
# Update the rmsprop memory.
rmsprop_cache = {k: np.zeros_like(v) for k, v in model.items()}
actors = [PongEnv.remote() for _ in range(batch_size)]
iteration = 0
while iteration != iterations:
    iteration += 1
    model_id = ray.put(model)
    actions = []
    # Launch tasks to compute gradients from multiple rollouts in parallel.
    start_time = time.time()
    for i in range(batch_size):
        action_id = actors[i].compute_gradient.remote(model_id)
        actions.append(action_id)
    for i in range(batch_size):
        action_id, actions = ray.wait(actions)
        grad, reward_sum = ray.get(action_id[0])
        # Accumulate the gradient over batch.
        for k in model:
            grad_buffer[k] += grad[k]
        running_reward = (reward_sum if running_reward is None else
                          running_reward * 0.99 + reward_sum * 0.01)
    end_time = time.time()
    print("Batch {} computed {} rollouts in {} seconds, "
          "running mean is {}".format(batch_num, batch_size,
                                      end_time - start_time,
                                      running_reward))
    for k, v in model.items():
        g = grad_buffer[k]
        rmsprop_cache[k] = (
            decay_rate * rmsprop_cache[k] + (1 - decay_rate) * g**2)
        model[k] += learning_rate * g / (np.sqrt(rmsprop_cache[k]) + 1e-5)
        # Reset the batch gradient buffer.
        grad_buffer[k] = np.zeros_like(v)
    batch_num += 1

# This code is copied and adapted from Andrej Karpathy's code for learning to
# play Pong https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5.

import numpy as np
import cPickle as pickle
import gym
import ray
import ray.services as services
import os

import functions

worker_dir = os.path.dirname(os.path.abspath(__file__))
worker_path = os.path.join(worker_dir, "worker.py")
services.start_singlenode_cluster(return_drivers=False, num_objstores=1, num_workers_per_objstore=10, worker_path=worker_path)

# hyperparameters
H = 200 # number of hidden layer neurons
batch_size = 10 # every how many episodes to do a param update?
learning_rate = 1e-4
decay_rate = 0.99 # decay factor for RMSProp leaky sum of grad^2
resume = False # resume from previous checkpoint?

running_reward = None
batch_num = 1
D = functions.D # input dimensionality: 80x80 grid
if resume:
  model = pickle.load(open("save.p", "rb"))
else:
  model = {}
  model["W1"] = np.random.randn(H, D) / np.sqrt(D) # "Xavier" initialization
  model["W2"] = np.random.randn(H) / np.sqrt(H)
grad_buffer = {k: np.zeros_like(v) for k, v in model.iteritems()} # update buffers that add up gradients over a batch
rmsprop_cache = {k: np.zeros_like(v) for k, v in model.iteritems()} # rmsprop memory

while True:
  modelref = ray.push(model)
  grads = []
  for i in range(batch_size):
    grads.append(functions.compgrad(modelref))
  for i in range(batch_size):
    grad = ray.pull(grads[i])
    for k in model: grad_buffer[k] += grad[0][k] # accumulate grad over batch
    running_reward = grad[1] if running_reward is None else running_reward * 0.99 + grad[1] * 0.01
    print "Batch {}. episode reward total was {}. running mean: {}".format(batch_num, grad[1], running_reward)
  for k, v in model.iteritems():
    g = grad_buffer[k] # gradient
    rmsprop_cache[k] = decay_rate * rmsprop_cache[k] + (1 - decay_rate) * g ** 2
    model[k] += learning_rate * g / (np.sqrt(rmsprop_cache[k]) + 1e-5)
    grad_buffer[k] = np.zeros_like(v) # reset batch gradient buffer
  batch_num += 1
  if batch_num % 10 == 0: pickle.dump(model, open("save.p", "wb"))

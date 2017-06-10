from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from tensorflow.contrib import nccl
from tensorflow.python.ops.data_flow_ops import StagingArea
from tensorflow.python.client import timeline
import numpy as np

import pickle
import time

from reinforce.models.visionnet import vision_net
from reinforce.distributions import Categorical
from reinforce.utils import iterate


PONG_V0_PICKLED_TRAJECTORY = "/home/ubuntu/Pong-v0-trajectory"
BATCH_SIZE = 1024
MAX_EXAMPLES = 20000

DEVICES = ["/cpu:0", "/cpu:1", "/cpu:2"]
NUM_DEVICES = len(DEVICES)
HAS_GPU = any(['gpu' in d for d in DEVICES])

def truncate(trajectory, size):
  batch = dict()
  for key in trajectory:
    batch[key] = trajectory[key][:size]
  return batch


def average_gradients(tower_grads):
  """Calculate the average gradient for each shared variable across all towers.
  Note that this function provides a synchronization point across all towers.
  Args:
    tower_grads: List of lists of (gradient, variable) tuples. The outer list
      is over individual gradients. The inner list is over the gradient
      calculation for each tower.
  Returns:
     List of pairs of (gradient, variable) where the gradient has been averaged
     across all towers.
  """

  average_grads = []
  for grad_and_vars in zip(*tower_grads):
    # Note that each grad_and_vars looks like the following:
    #   ((grad0_gpu0, var0_gpu0), ... , (grad0_gpuN, var0_gpuN))
    grads = []
    for g, _ in grad_and_vars:
      if g is not None:
        # Add 0 dimension to the gradients to represent the tower.
        expanded_g = tf.expand_dims(g, 0)

        # Append on a 'tower' dimension which we will average over below.
        grads.append(expanded_g)

    if grads:
      # Average over the 'tower' dimension.
      grad = tf.concat(axis=0, values=grads)
      grad = tf.reduce_mean(grad, 0)

      # Keep in mind that the Variables are redundant because they are shared
      # across towers. So .. we will just return the first tower's pointer to
      # the Variable.
      v = grad_and_vars[0][1]
      grad_and_var = (grad, v)
      average_grads.append(grad_and_var)
  return average_grads


# For Pong-v0
observations = tf.placeholder(tf.float32, shape=(None, 80, 80, 3))
prev_logits = tf.placeholder(tf.float32, shape=(None, 6))
actions = tf.placeholder(tf.int64, shape=(None,))

def create_loss(observations, prev_logits, actions):
  curr_logits = vision_net(observations, num_classes=6)
  curr_dist = Categorical(curr_logits)
  prev_dist = Categorical(prev_logits)
  ratio = tf.exp(curr_dist.logp(actions) - prev_dist.logp(actions))
  loss = tf.reduce_mean(ratio)
  return loss

dummy_loss = create_loss(observations, prev_logits, actions)
optimizer = tf.train.AdamOptimizer(5e-5)
grad = optimizer.compute_gradients(dummy_loss)
train_op = optimizer.apply_gradients(grad)
mean_loss = tf.reduce_mean(dummy_loss)

trajectory = truncate(
    pickle.load(open(PONG_V0_PICKLED_TRAJECTORY, 'rb')), MAX_EXAMPLES)

total_examples = len(trajectory["observations"])
print("Total examples", total_examples)

def make_inputs(batch):
    return {
          observations: batch["observations"],
          actions: batch["actions"].squeeze(),
          prev_logits: batch["logprobs"]
        }

def run(ops, i, feed_dict={}, trace_as=None):
  start = time.time()
  full_trace = trace_as and i == 2
  if full_trace:
    run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
  else:
    run_options = tf.RunOptions(trace_level=tf.RunOptions.NO_TRACE)
  run_metadata = tf.RunMetadata()
  sess.run(
      ops, feed_dict=feed_dict, options=run_options, run_metadata=run_metadata)
  if full_trace:
    trace = timeline.Timeline(step_stats=run_metadata.step_stats)
    out = '/tmp/ray/timeline-{}.json'.format(trace_as)
    trace_file = open(out, 'w')
    trace_file.write(trace.generate_chrome_trace_format())
    print("Wrote trace file to", out)
  print("iteration", i, time.time() - start, "seconds")

def baseline_strategy(trajectory):
  print("Current loss", sess.run(mean_loss, feed_dict=make_inputs(trajectory)))
  start = time.time()
  for i, batch in enumerate(iterate(trajectory, BATCH_SIZE)):
    run(train_op, i, make_inputs(batch), trace_as="baseline")
  delta = time.time() - start
  print("Final loss", sess.run(mean_loss, feed_dict=make_inputs(trajectory)))
  return delta

def run_experiment(strategy, name):
  print()
  print("*** Running experiment", name)
  sess.run(tf.global_variables_initializer())
  delta = strategy(trajectory)
  print("Examples per second", total_examples / delta)


#run_experiment(baseline_strategy, "Baseline")

# use variables to feed

def variables_strategy_net(device, index, reuse_variables):
  with tf.device(device):
    with tf.variable_scope("variables_strategy_data_loading"):
      # placeholders to load the whole dataset into the gpu
      observations_initializer = tf.placeholder(tf.float32, shape=(MAX_EXAMPLES, 80, 80, 3))
      prev_logits_initializer = tf.placeholder(tf.float32, shape=(MAX_EXAMPLES, 6))
      actions_initializer = tf.placeholder(tf.int64, shape=(MAX_EXAMPLES,))
      
      observations_data = tf.Variable(
        observations_initializer, trainable=False, collections=[])
      prev_logits_data = tf.Variable(
        prev_logits_initializer, trainable=False, collections=[])
      actions_data = tf.Variable(
        actions_initializer, trainable=False, collections=[])
 
    with tf.variable_scope("variables_strategy_net"):
      if reuse_variables:
        tf.get_variable_scope().reuse_variables()
      observations_batch = tf.slice(observations_data, [index, 0, 0, 0], [BATCH_SIZE, -1, -1, -1])
      prev_logits_batch = tf.slice(prev_logits_data, [index, 0], [BATCH_SIZE, -1])
      actions_batch = tf.slice(actions_data, [index], [BATCH_SIZE])
      print("Observations batch", observations_batch)
 
      dummy_loss = create_loss(observations_batch, prev_logits_batch, actions_batch)
      optimizer = tf.train.AdamOptimizer(5e-5)
      grad = optimizer.compute_gradients(dummy_loss)
    
      mean_loss = tf.reduce_mean(dummy_loss)
  
  return observations_data, prev_logits_data, actions_data, observations_initializer, prev_logits_initializer, actions_initializer, grad, mean_loss

index = tf.placeholder(tf.int32)
grads = []

config_proto = tf.ConfigProto(device_count={"CPU": 4}, log_device_placement=True, allow_soft_placement=True)
sess = tf.Session(config=config_proto)
s = time.time()
for i in range(16):
	observations_data, prev_logits_data, actions_data, observations_initializer, prev_logits_initializer, actions_initializer, grad, mean_loss = variables_strategy_net(
		"/gpu:" + str(i), index, reuse_variables=i > 0)
	t2 = time.time()
	sess.run([observations_data.initializer, prev_logits_data.initializer, actions_data.initializer],
		 feed_dict={observations_initializer: trajectory["observations"],
			    actions_initializer: trajectory["actions"].squeeze(),
			    prev_logits_initializer: trajectory["logprobs"]})
	print("Load", i, time.time() - t2)
	grads.append(grad)
print("Data load takes", time.time() - s)

optimizer = tf.train.AdamOptimizer(5e-5)
train_op = optimizer.apply_gradients(average_gradients(grads))

def variables_strategy(trajectory):
  print("Current loss", sess.run(mean_loss, feed_dict=dict(list({index: 0}.items()) + list(make_inputs(trajectory).items()))))
  start = time.time()
  for i, batch in enumerate(iterate(trajectory, BATCH_SIZE)):
    run(train_op, i, {index: i}, trace_as="variables")
  delta = time.time() - start
  print("Final loss", sess.run(mean_loss, feed_dict=dict(list({index: 0}.items()) + list(make_inputs(trajectory).items()))))
  return delta

run_experiment(variables_strategy, "TensorFlow Variables")

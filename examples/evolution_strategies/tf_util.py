# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import functools
import os

# Tensorflow must be at least version 1.0.0 for the example to work.
if int(tf.__version__.split(".")[0]) < 1:
  raise Exception("Your Tensorflow version is less than 1.0.0. Please update "
                  "Tensorflow to the latest version.")

# ================================================================
# Import all names into common namespace
# ================================================================

clip = tf.clip_by_value

# Make consistent with numpy


def sum(x, axis=None, keepdims=False):
  return tf.reduce_sum(x, reduction_indices=None if axis is None else [axis],
                       keep_dims=keepdims)


def mean(x, axis=None, keepdims=False):
  return tf.reduce_mean(x, reduction_indices=None if axis is None else [axis],
                        keep_dims=keepdims)


def var(x, axis=None, keepdims=False):
  meanx = mean(x, axis=axis, keepdims=keepdims)
  return mean(tf.square(x - meanx), axis=axis, keepdims=keepdims)


def std(x, axis=None, keepdims=False):
  return tf.sqrt(var(x, axis=axis, keepdims=keepdims))


def max(x, axis=None, keepdims=False):
  return tf.reduce_max(x, reduction_indices=None if axis is None else [axis],
                       keep_dims=keepdims)


def min(x, axis=None, keepdims=False):
  return tf.reduce_min(x, reduction_indices=None if axis is None else [axis],
                       keep_dims=keepdims)


def concatenate(arrs, axis=0):
  return tf.concat(arrs, axis)


def argmax(x, axis=None):
  return tf.argmax(x, dimension=axis)

# Extras


def l2loss(params):
  if len(params) == 0:
    return tf.constant(0.0)
  else:
    return tf.add_n([sum(tf.square(p)) for p in params])


def lrelu(x, leak=0.2):
  f1 = 0.5 * (1 + leak)
  f2 = 0.5 * (1 - leak)
  return f1 * x + f2 * abs(x)


def categorical_sample_logits(X):
  # https://github.com/tensorflow/tensorflow/issues/456
  U = tf.random_uniform(tf.shape(X))
  return argmax(X - tf.log(-tf.log(U)), axis=1)

# Global session


def get_session():
  return tf.get_default_session()


def single_threaded_session():
  tf_config = tf.ConfigProto(inter_op_parallelism_threads=1,
                             intra_op_parallelism_threads=1)
  return tf.Session(config=tf_config)


ALREADY_INITIALIZED = set()


def initialize():
  new_variables = set(tf.global_variables()) - ALREADY_INITIALIZED
  get_session().run(tf.variables_initializer(new_variables))
  ALREADY_INITIALIZED.update(new_variables)


def eval(expr, feed_dict=None):
  if feed_dict is None:
    feed_dict = {}
  return get_session().run(expr, feed_dict=feed_dict)


def set_value(v, val):
  get_session().run(v.assign(val))


def load_state(fname):
  saver = tf.train.Saver()
  saver.restore(get_session(), fname)


def save_state(fname):
  os.makedirs(os.path.dirname(fname), exist_ok=True)
  saver = tf.train.Saver()
  saver.save(get_session(), fname)

# Model components


def normc_initializer(std=1.0):
  def _initializer(shape, dtype=None, partition_info=None):
    out = np.random.randn(*shape).astype(np.float32)
    out *= std / np.sqrt(np.square(out).sum(axis=0, keepdims=True))
    return tf.constant(out)
  return _initializer


def dense(x, size, name, weight_init=None, bias=True):
  w = tf.get_variable(name + "/w", [x.get_shape()[1], size],
                      initializer=weight_init)
  ret = tf.matmul(x, w)
  if bias:
    b = tf.get_variable(name + "/b", [size],
                        initializer=tf.zeros_initializer())
    return ret + b
  else:
    return ret

# Basic Stuff


def function(inputs, outputs, updates=None, givens=None):
  if isinstance(outputs, list):
    return _Function(inputs, outputs, updates, givens=givens)
  elif isinstance(outputs, dict):
    f = _Function(inputs, outputs.values(), updates, givens=givens)
    return lambda *inputs: dict(zip(outputs.keys(), f(*inputs)))
  else:
    f = _Function(inputs, [outputs], updates, givens=givens)
    return lambda *inputs: f(*inputs)[0]


class _Function(object):
  def __init__(self, inputs, outputs, updates, givens, check_nan=False):
    assert all(len(i.op.inputs) == 0 for i in inputs), ("inputs should all be "
                                                        "placeholders")
    self.inputs = inputs
    updates = updates or []
    self.update_group = tf.group(*updates)
    self.outputs_update = list(outputs) + [self.update_group]
    self.givens = {} if givens is None else givens
    self.check_nan = check_nan

  def __call__(self, *inputvals):
    assert len(inputvals) == len(self.inputs)
    feed_dict = dict(zip(self.inputs, inputvals))
    feed_dict.update(self.givens)
    results = get_session().run(self.outputs_update, feed_dict=feed_dict)[:-1]
    if self.check_nan:
      if any(np.isnan(r).any() for r in results):
        raise RuntimeError("Nan detected")
    return results


# Graph traversal

VARIABLES = {}

# Flat vectors


def var_shape(x):
  out = [k.value for k in x.get_shape()]
  assert all(isinstance(a, int) for a in out), ("shape function assumes that "
                                                "shape is fully known")
  return out


def numel(x):
  return intprod(var_shape(x))


def intprod(x):
  return int(np.prod(x))


def flatgrad(loss, var_list):
  grads = tf.gradients(loss, var_list)
  return tf.concat([tf.reshape(grad, [numel(v)], 0)
                    for (v, grad) in zip(var_list, grads)])


class SetFromFlat(object):
  def __init__(self, var_list, dtype=tf.float32):
    assigns = []
    shapes = list(map(var_shape, var_list))
    total_size = np.sum([intprod(shape) for shape in shapes])

    self.theta = theta = tf.placeholder(dtype, [total_size])
    start = 0
    assigns = []
    for (shape, v) in zip(shapes, var_list):
      size = intprod(shape)
      assigns.append(tf.assign(v, tf.reshape(theta[start:start + size],
                                             shape)))
      start += size
    assert start == total_size
    self.op = tf.group(*assigns)

  def __call__(self, theta):
    get_session().run(self.op, feed_dict={self.theta: theta})


class GetFlat(object):
  def __init__(self, var_list):
    self.op = tf.concat([tf.reshape(v, [numel(v)]) for v in var_list], 0)

  def __call__(self):
    return get_session().run(self.op)

# Misc


def scope_vars(scope, trainable_only):
  """Get variables inside a scope. The scope can be specified as a string."""
  return tf.get_collection((tf.GraphKeys.TRAINABLE_VARIABLES if trainable_only
                            else tf.GraphKeys.GLOBAL_VARIABLES),
                           scope=(scope if isinstance(scope, str)
                                  else scope.name))


def in_session(f):
  @functools.wraps(f)
  def newfunc(*args, **kwargs):
    with tf.Session():
      f(*args, **kwargs)
  return newfunc


# A mapping from name -> (placeholder, dtype, shape).
_PLACEHOLDER_CACHE = {}


def get_placeholder(name, dtype, shape):
  print("calling get_placeholder", name)
  if name in _PLACEHOLDER_CACHE:
    out, dtype1, shape1 = _PLACEHOLDER_CACHE[name]
    assert dtype1 == dtype and shape1 == shape
    return out
  else:
    out = tf.placeholder(dtype=dtype, shape=shape, name=name)
    _PLACEHOLDER_CACHE[name] = (out, dtype, shape)
    return out


def get_placeholder_cached(name):
  return _PLACEHOLDER_CACHE[name][0]


def flattenallbut0(x):
  return tf.reshape(x, [-1, intprod(x.get_shape().as_list()[1:])])


def reset():
  global _PLACEHOLDER_CACHE
  global VARIABLES
  _PLACEHOLDER_CACHE = {}
  VARIABLES = {}
  tf.reset_default_graph()

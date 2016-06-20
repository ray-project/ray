import numpy as np
import ray

import tensorflow as tf

image_dimension = 784
label_dimension = 10
w_shape = [image_dimension, label_dimension]
w_size = np.prod(w_shape)
b_shape = [label_dimension]
b_size = np.prod(b_shape)
dim = w_size + b_size

x = tf.placeholder(tf.float32, [None, image_dimension])
w = tf.Variable(tf.zeros(w_shape))
b = tf.Variable(tf.zeros(b_shape))
y = tf.nn.softmax(tf.matmul(x, w) + b)
y_ = tf.placeholder(tf.float32, [None, label_dimension])
cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))
cross_entropy_grads = tf.gradients(cross_entropy, [w, b])

w_new = tf.placeholder(tf.float32, w_shape)
b_new = tf.placeholder(tf.float32, b_shape)
update_w = w.assign(w_new)
update_b = b.assign(b_new)

init = tf.initialize_all_variables()
sess = tf.Session()
sess.run(init)

def load_weights(theta):
  sess.run([update_w, update_b], feed_dict={w_new: theta[:w_size].reshape(w_shape), b_new: theta[w_size:]})

@ray.remote([np.ndarray, np.ndarray, np.ndarray], [float])
def loss(theta, xs, ys):
  load_weights(theta)
  return float(sess.run(cross_entropy, feed_dict={x: xs, y_: ys}))

@ray.remote([np.ndarray, np.ndarray, np.ndarray], [np.ndarray])
def grad(theta, xs, ys):
  load_weights(theta)
  gradients = sess.run(cross_entropy_grads, feed_dict={x: xs, y_: ys})
  return np.concatenate([g.flatten() for g in gradients])

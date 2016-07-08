# Most of the tensorflow code is adapted from Tensorflow's tutorial on using CNNs to train MNIST
# https://www.tensorflow.org/versions/r0.9/tutorials/mnist/pros/index.html#build-a-multilayer-convolutional-network
import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data
import numpy as np
import ray

mnist = input_data.read_data_sets("MNIST_data", one_hot=True)

def weight(shape, stddev):
  initial = tf.truncated_normal(shape, stddev=stddev)
  return tf.Variable(initial)

def bias(shape):
  initial = tf.constant(0.1, shape=shape)
  return tf.Variable(initial)

def conv2d(x, W):
  return tf.nn.conv2d(x, W, strides=[1, 1, 1, 1], padding="SAME")

def max_pool_2x2(x):
  return tf.nn.max_pool(x, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding="SAME")

@ray.remote([dict, int], [float])
def train_cnn(params, epochs):
  learning_rate = params["learning_rate"]
  batch_size = params["batch_size"]
  keep = 1 - params["dropout"]
  stddev = params["stddev"]
  x = tf.placeholder(tf.float32, shape=[None, 784])
  y = tf.placeholder(tf.float32, shape=[None, 10])
  keep_prob = tf.placeholder(tf.float32)
  train_step, accuracy = cnn_setup(x, y, keep_prob, learning_rate, stddev)
  with tf.Session() as sess:
    sess.run(tf.initialize_all_variables())
    for i in range(1, epochs):
      batch = mnist.train.next_batch(batch_size)
      sess.run(train_step, feed_dict={x: batch[0], y: batch[1], keep_prob: keep})
      if i % 100 == 0: # checks if accuracy is low enough to stop early every set number of epochs
        train_ac = accuracy.eval(feed_dict={x: batch[0], y: batch[1], keep_prob: 1.0})
        if train_ac < 0.25: # Accuracy threshold is on a application to application basis.
          totalacc = accuracy.eval(feed_dict={x: mnist.validation.images, y: mnist.validation.labels, keep_prob: 1.0})
          return totalacc
    totalacc = accuracy.eval(feed_dict={x: mnist.validation.images, y: mnist.validation.labels, keep_prob: 1.0})
  return totalacc.astype("float64")

def cnn_setup(x, y, keep_prob, lr, stddev):
  first_hidden = 32
  second_hidden = 64
  fc_hidden = 1024
  W_conv1 = weight([5, 5, 1, first_hidden], stddev)
  B_conv1 = bias([first_hidden])
  x_image = tf.reshape(x, [-1, 28, 28, 1])
  h_conv1 = tf.nn.relu(conv2d(x_image, W_conv1) + B_conv1)
  h_pool1 = max_pool_2x2(h_conv1)
  W_conv2 = weight([5, 5, first_hidden, second_hidden], stddev)
  b_conv2 = bias([second_hidden])
  h_conv2 = tf.nn.relu(conv2d(h_pool1, W_conv2) + b_conv2)
  h_pool2 = max_pool_2x2(h_conv2)
  W_fc1 = weight([7 * 7 * second_hidden, fc_hidden], stddev)
  b_fc1 = bias([fc_hidden])
  h_pool2_flat = tf.reshape(h_pool2, [-1, 7 * 7 * second_hidden])
  h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)
  h_fc1_drop= tf.nn.dropout(h_fc1, keep_prob)
  W_fc2 = weight([fc_hidden, 10], stddev)
  b_fc2 = bias([10])
  y_conv = tf.nn.softmax(tf.matmul(h_fc1_drop, W_fc2) + b_fc2)
  cross_entropy = tf.reduce_mean(-tf.reduce_sum(y * tf.log(y_conv), reduction_indices=[1]))
  correct_pred = tf.equal(tf.argmax(y_conv, 1), tf.argmax(y, 1))
  return tf.train.AdamOptimizer(lr).minimize(cross_entropy), tf.reduce_mean(tf.cast(correct_pred, tf.float32))

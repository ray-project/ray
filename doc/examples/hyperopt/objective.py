# Most of the tensorflow code is adapted from Tensorflow's tutorial on using
# CNNs to train MNIST
# https://www.tensorflow.org/versions/r0.9/tutorials/mnist/pros/index.html#build-a-multilayer-convolutional-network.  # noqa: E501

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

import ray
import ray.experimental.tf_utils


def get_batch(data, batch_index, batch_size):
    # This method currently drops data when num_data is not divisible by
    # batch_size.
    num_data = data.shape[0]
    num_batches = num_data // batch_size
    batch_index %= num_batches
    return data[(batch_index * batch_size):((batch_index + 1) * batch_size)]


def weight(shape, stddev):
    initial = tf.truncated_normal(shape, stddev=stddev)
    return tf.Variable(initial)


def bias(shape):
    initial = tf.constant(0.1, shape=shape)
    return tf.Variable(initial)


def conv2d(x, W):
    return tf.nn.conv2d(x, W, strides=[1, 1, 1, 1], padding="SAME")


def max_pool_2x2(x):
    return tf.nn.max_pool(
        x, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding="SAME")


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
    h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)
    W_fc2 = weight([fc_hidden, 10], stddev)
    b_fc2 = bias([10])
    y_conv = tf.nn.softmax(tf.matmul(h_fc1_drop, W_fc2) + b_fc2)
    cross_entropy = tf.reduce_mean(
        -tf.reduce_sum(y * tf.log(y_conv), reduction_indices=[1]))
    correct_pred = tf.equal(tf.argmax(y_conv, 1), tf.argmax(y, 1))
    return (tf.train.AdamOptimizer(lr).minimize(cross_entropy),
            tf.reduce_mean(tf.cast(correct_pred, tf.float32)), cross_entropy)


# Define a remote function that takes a set of hyperparameters as well as the
# data, consructs and trains a network, and returns the validation accuracy.
@ray.remote
def train_cnn_and_compute_accuracy(params,
                                   steps,
                                   train_images,
                                   train_labels,
                                   validation_images,
                                   validation_labels,
                                   weights=None):
    # Extract the hyperparameters from the params dictionary.
    learning_rate = params["learning_rate"]
    batch_size = params["batch_size"]
    keep = 1 - params["dropout"]
    stddev = params["stddev"]
    # Create the network and related variables.
    with tf.Graph().as_default():
        # Create the input placeholders for the network.
        x = tf.placeholder(tf.float32, shape=[None, 784])
        y = tf.placeholder(tf.float32, shape=[None, 10])
        keep_prob = tf.placeholder(tf.float32)
        # Create the network.
        train_step, accuracy, loss = cnn_setup(x, y, keep_prob, learning_rate,
                                               stddev)
        # Do the training and evaluation.
        with tf.Session() as sess:
            # Use the TensorFlowVariables utility. This is only necessary if we
            # want to set and get the weights.
            variables = ray.experimental.tf_utils.TensorFlowVariables(
                loss, sess)
            # Initialize the network weights.
            sess.run(tf.global_variables_initializer())
            # If some network weights were passed in, set those.
            if weights is not None:
                variables.set_weights(weights)
            # Do some steps of training.
            for i in range(1, steps + 1):
                # Fetch the next batch of data.
                image_batch = get_batch(train_images, i, batch_size)
                label_batch = get_batch(train_labels, i, batch_size)
                # Do one step of training.
                sess.run(
                    train_step,
                    feed_dict={
                        x: image_batch,
                        y: label_batch,
                        keep_prob: keep
                    })
            # Training is done, so compute the validation accuracy and the
            # current weights and return.
            totalacc = accuracy.eval(feed_dict={
                x: validation_images,
                y: validation_labels,
                keep_prob: 1.0
            })
            new_weights = variables.get_weights()
    return float(totalacc), new_weights

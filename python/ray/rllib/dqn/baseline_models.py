# TODO(ekl) remove before merging

import tensorflow as tf
import tensorflow.contrib.layers as layers


def _mlp(hiddens, inpt, num_actions, layer_norm=False):
    out = inpt
    for hidden in hiddens:
        out = layers.fully_connected(out, num_outputs=hidden, activation_fn=None)
        if layer_norm:
            out = layers.layer_norm(out, center=True, scale=True)
        out = tf.nn.relu(out)
    q_out = layers.fully_connected(out, num_outputs=num_actions, activation_fn=None)
    return q_out


def mlp(hiddens=[], layer_norm=False):
    """This model takes as input an observation and returns values of all actions.
    Parameters
    ----------
    hiddens: [int]
        list of sizes of hidden layers
    Returns
    -------
    q_func: function
        q_function for DQN algorithm.
    """
    return lambda *args, **kwargs: _mlp(hiddens, layer_norm=layer_norm, *args, **kwargs)


def _cnn_to_mlp(convs, hiddens, dueling, inpt, num_actions, layer_norm=False):
    out = inpt
    with tf.variable_scope("convnet"):
        for num_outputs, kernel_size, stride in convs:
            out = layers.convolution2d(out,
                                       num_outputs=num_outputs,
                                       kernel_size=kernel_size,
                                       stride=stride,
                                       activation_fn=tf.nn.relu)
    conv_out = layers.flatten(out)
    with tf.variable_scope("action_value"):
        action_out = conv_out
        for hidden in hiddens:
            action_out = layers.fully_connected(action_out, num_outputs=hidden, activation_fn=None)
            if layer_norm:
                action_out = layers.layer_norm(action_out, center=True, scale=True)
            action_out = tf.nn.relu(action_out)
        action_scores = layers.fully_connected(action_out, num_outputs=num_actions, activation_fn=None)

    if dueling:
        with tf.variable_scope("state_value"):
            state_out = conv_out
            for hidden in hiddens:
                state_out = layers.fully_connected(state_out, num_outputs=hidden, activation_fn=None)
                if layer_norm:
                    state_out = layers.layer_norm(state_out, center=True, scale=True)
                state_out = tf.nn.relu(state_out)
            state_score = layers.fully_connected(state_out, num_outputs=1, activation_fn=None)
        action_scores_mean = tf.reduce_mean(action_scores, 1)
        action_scores_centered = action_scores - tf.expand_dims(action_scores_mean, 1)
        q_out = state_score + action_scores_centered
    else:
        q_out = action_scores
    return q_out


def cnn_to_mlp(convs, hiddens, dueling=False, layer_norm=False):
    """This model takes as input an observation and returns values of all actions.
    Parameters
    ----------
    convs: [(int, int int)]
        list of convolutional layers in form of
        (num_outputs, kernel_size, stride)
    hiddens: [int]
        list of sizes of hidden layers
    dueling: bool
        if true double the output MLP to compute a baseline
        for action scores
    Returns
    -------
    q_func: function
        q_function for DQN algorithm.
    """

    return lambda *args, **kwargs: _cnn_to_mlp(convs, hiddens, dueling, layer_norm=layer_norm, *args, **kwargs)

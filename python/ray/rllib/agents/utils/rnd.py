""" Implementation of Random Network Distillation(RND).

Author
------
Vishal Satish
"""
import logging
from functools import reduce

import tensorflow as tf
import numpy as np
import IPython as ip

from ray.rllib.utils.filter import get_filter

logger = logging.getLogger(__name__)

def flatten(x):
    return tf.reshape(x, (-1, reduce(lambda x, y: x*y, x.get_shape().as_list()[1:])))

def kaiming_he_initializer(fan_in):
    return tf.initializers.truncated_normal(stddev=np.sqrt(2.0 / fan_in))

def build_conv2d(x,
                 filt_dim=(4, 4), 
                 num_filts=16, 
                 filt_stride=(1, 1, 1, 1), 
                 padding='VALID',
                 activation_fn = tf.nn.relu, 
                 name='conv'):
    logger.info('Building Conv2D Layer: {}...'.format(name))

    # initialize weights
    with tf.variable_scope(name):
        input_channels = x.get_shape().as_list()[-1]
        fan_in = reduce(lambda x, y: x*y, filt_dim) * input_channels
        W = tf.get_variable('weights', shape=filt_dim + (input_channels, num_filts), initializer=kaiming_he_initializer(fan_in))
        b = tf.get_variable('bias', shape=(num_filts,), initializer=kaiming_he_initializer(fan_in))

    # build layer
    out = x
    out = tf.nn.conv2d(out, W, filt_stride, padding, name=name) + b
    out = activation_fn(out)
    
    return out
    
def build_fc(x,
             out_size,
             activation_fn=tf.nn.relu,
             name='fc'):
    logger.info('Building FC Layer: {}'.format(name))

    # initialize weights
    with tf.variable_scope(name):
        fan_in = x.get_shape().as_list()[-1]
        std = np.sqrt(2.0 / fan_in)
        W = tf.get_variable('weights', shape=(fan_in, out_size), initializer=kaiming_he_initializer(fan_in))
        b = tf.get_variable('bias', shape=(out_size,), initializer=kaiming_he_initializer(fan_in))

    # build layer
    out = x
    out = tf.matmul(out, W) + b
    out = activation_fn(out)

    return out

class RND(object):
    DEFAULT_CONV_MODEL = {
                        'conv1' : {
                            'type' : 'conv',
                            'filt_dim' : 8,
                            'num_filts' : 16,
                            'padding' : 'VALID',
                            'filt_stride' : 4,
                            'activation' : 'relu',
                        },
                        'conv2' : {
                            'type' : 'conv',
                            'filt_dim' : 4,
                            'num_filts' : 32,
                            'padding' : 'VALID',
                            'filt_stride' : 2,
                            'activation' : 'relu',
                        },
                        'conv3' : {
                            'type' : 'conv',
                            'filt_dim' : 3,
                            'num_filts' : 32,
                            'padding' : 'VALID',
                            'filt_stride' : 1,
                            'activation' : 'relu',
                        },
                        'fc1' : {
                            'type' : 'fc',
                            'out_size' : 512,
                            'activation' : 'relu',
                        }
                    }

    DEFAULT_FC_MODEL = {
                        'fc1' : {
                            'type' : 'fc',
                            'out_size' : 512,
                            'activation' : 'relu',
                        }
                       }

    def __init__(self, obs, model=DEFAULT_CONV_MODEL, rnd_predictor_update_proportion=1.0):
        self._rnd_predictor_update_proportion = rnd_predictor_update_proportion
        self._model = model
   
        # normalize and clip obs to [-5.0, 5.0]
        obs = tf.clip_by_value(obs, -5.0, 5.0)
 
        # build networks
        self._targets = self._build_network(obs, self._model, name='target_network')
        self._preds = self._build_network(obs, self._model, name='predictor_network')

        # build intr reward (bonus) with normalization
        self._intr_reward = self._build_intr_reward(self._targets, self._preds)

        # build loss for random network
        self._loss = self._build_loss(self._targets, self._preds)

    @property
    def intr_rew(self):
        return self._intr_reward

    @property
    def loss(self):
        return self._loss

    def _build_intr_reward(self, targets, preds):
        logger.info('Building intrinisic reward...')
        targets = tf.stop_gradient(targets) #TODO: Is this really needed?
        intr_rew = tf.reduce_mean(tf.square(preds - targets), axis=-1, keep_dims=True)
        return intr_rew

    def _build_loss(self, targets, preds):
        logger.info('Building loss...')
        targets = tf.stop_gradient(targets)
        loss = tf.reduce_mean(tf.square(preds - targets), axis=-1)
        keep_mask = tf.random_uniform(shape=tf.shape(loss), minval=0.0, maxval=1.0, dtype=tf.float32)
        keep_mask = tf.cast(keep_mask < self._rnd_predictor_update_proportion, tf.float32)
        loss = tf.reduce_sum(loss * keep_mask) / tf.maximum(tf.reduce_sum(keep_mask), 1.0)
        return loss

    def _build_network(self, x, model, name='network'):
        logger.info('Building {}...'.format(name))
   
        # build network
        out = x
        prev_layer_type = None
        with tf.variable_scope(name):
            for layer, cfg in model.items():
                if cfg['type'] == 'conv':
                    assert prev_layer_type is not 'fc', 'Cannot have conv layer after fc layer!'
                    out = build_conv2d(out, 
                                       (cfg['filt_dim'], cfg['filt_dim']),
                                       cfg['num_filts'],
                                       (1, cfg['filt_stride'], cfg['filt_stride'], 1),
                                       cfg['padding'],
                                       {'relu' : tf.nn.relu}[cfg['activation']],
                                       layer)
                elif cfg['type'] == 'fc':
                    if prev_layer_type == 'conv':
                        out = flatten(out)
                    out = build_fc(out,
                                   cfg['out_size'],
                                   {'relu' : tf.nn.relu}[cfg['activation']],
                                   layer)
                else:
                    raise ValueError('Invalid layer type: {}'.format(cfg['type']))
                prev_layer_type = cfg['type']
        return out

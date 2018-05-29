from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import gym

from ray.rllib.models.impalanet import ImpalaNet
from ray.rllib.models import ModelCatalog


def test_forward():
    env = gym.make("PongNoFrameskip-v4")
    ob_space = env.observation_space.shape
    ac_space = env.action_space

    x = tf.placeholder(tf.float32, [None] + list(ob_space))
    dist_class, logit_dim = ModelCatalog.get_action_dist(ac_space)
    model = ImpalaNet(x, logit_dim, {"traj_length": 3})

    state_init = model.state_init
    state_in = model.state_in
    state_out = model.state_out
    logits = model.outputs
    curr_dist = dist_class(logits)

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())
    
    last_state = env.reset()
    for _ in range(3):
        action = sess.run(curr_dist, feed_dict={x: [last_state]})
        print(action)

if __name__=="__main__":
    test_forward()

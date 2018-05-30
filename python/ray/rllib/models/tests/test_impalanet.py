from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import gym

from ray.rllib.models.impalanet import ImpalaShadowNet
from ray.rllib.models import ModelCatalog


def test_shadow():
    env_model = {
        "grayscale": True,
        "zero_mean": False,
        "dim": 80,
        "channel_major": False
    }
    env_creator = lambda env_config: gym.make("PongNoFrameskip-v4")
    env = ModelCatalog.get_preprocessor_as_wrapper(
            None, env_creator(""), env_model)
    ob_space = env.observation_space.shape
    ac_space = env.action_space

    num_trajs = 2
    traj_length = 3
    x = tf.placeholder(tf.float32, [None] + list(ob_space))
    a = tf.placeholder(tf.int64, [None])
    dist_class, logit_dim = ModelCatalog.get_action_dist(ac_space)
    model = ImpalaShadowNet(x, logit_dim, {"traj_length": traj_length, "num_trajs": num_trajs})

    state_init = model.state_init
    state_in = model.state_in
    state_out = model.state_out
    curr_dist = dist_class(model.outputs)
    sample = curr_dist.sample()
    curr_dists = dist_class(model.logits)
    logps = curr_dists.logp(a)
    

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())
    print("session created successfully")
    
    obs = list()
    features = list()
    actions = list()
    last_state = env.reset()
    last_features = state_init
    done = False
    num_step = 0
    while not done:
        action, c, h = sess.run([sample] + state_out, feed_dict={x: [last_state], state_in[0]: last_features[0], state_in[1]: last_features[1]})
        next_state, rwd, done, info = env.step(action)
        
        obs.append(last_state)
        actions.append(action)
        if num_step % traj_length == 0:
            features.append(last_features)
        num_step += 1
        last_state = next_state
        last_features = (c, h)

        if num_step >= (num_trajs*traj_length):
            break

    logp_vals, last_layers = sess.run(
        [logps, model.last_layers],
        feed_dict={
            x: obs,
            a: np.squeeze(actions),
            model.state_ins[0]: [f[0][0] for f in features],
            model.state_ins[1]: [f[1][0] for f in features]})
    print(logp_vals)
    print(last_layers.shape)

if __name__=="__main__":
    test_shadow()

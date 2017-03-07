import gym.spaces
import tensorflow as tf
import tensorflow.contrib.slim as slim
from reinforce.models.visionnet import vision_net
from reinforce.models.fcnet import fc_net
from reinforce.distributions import Categorical, DiagGaussian

class ProximalPolicyLoss(object):

  def __init__(self, observation_space, action_space, config, sess):
    assert isinstance(action_space, gym.spaces.Discrete) or isinstance(action_space, gym.spaces.Box)
    # adapting the kl divergence
    self.kl_coeff = tf.placeholder(name='newkl', shape=(), dtype=tf.float32)
    self.observations = tf.placeholder(tf.float32, shape=(None,) + observation_space.shape)
    self.advantages = tf.placeholder(tf.float32, shape=(None,))

    if isinstance(action_space, gym.spaces.Box):
      # First half of the dimensions are the means, the second half are the standard deviations
      self.action_dim = action_space.shape[0]
      self.logit_dim = 2 * self.action_dim
      self.actions = tf.placeholder(tf.float32, shape=(None, action_space.shape[0]))
      Distribution = DiagGaussian
    elif isinstance(action_space, gym.spaces.Discrete):
      self.action_dim = action_space.n
      self.logit_dim = self.action_dim
      self.actions = tf.placeholder(tf.int64, shape=(None,))
      Distribution = Categorical
    else:
      raise NotImplemented("action space" + str(type(env.action_space)) + "currently not supported")
    self.prev_logits = tf.placeholder(tf.float32, shape=(None, self.logit_dim))
    self.prev_dist = Distribution(self.prev_logits)
    self.curr_logits = fc_net(self.observations, num_classes=self.logit_dim)
    self.curr_dist = Distribution(self.curr_logits)
    self.sampler = self.curr_dist.sample()
    self.entropy = self.curr_dist.entropy()
    # Make loss functions.
    self.ratio = tf.exp(self.curr_dist.logp(self.actions) - self.prev_dist.logp(self.actions))
    self.kl = self.prev_dist.kl(self.curr_dist)
    self.mean_kl = tf.reduce_mean(self.kl)
    self.mean_entropy = tf.reduce_mean(self.entropy)
    # XXX
    self.surr1 = self.ratio * self.advantages
    self.surr2 = tf.clip_by_value(self.ratio, 1 - config["clip_param"], 1 + config["clip_param"]) * self.advantages
    self.surr = tf.minimum(self.surr1, self.surr2)
    self.loss = tf.reduce_mean(-self.surr + self.kl_coeff * self.kl - config["entropy_coeff"] * self.entropy)
    self.sess = sess

  def compute_actions(self, observations):
    return self.sess.run([self.sampler, self.curr_logits], feed_dict={self.observations: observations})

  def loss(self):
    return self.loss
                                                             self.prev_logits: prev_logits})

import logging
import numpy as np
import math
from ray.rllib.utils.framework import try_import_tf, try_import_tfp

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)

def compute_l2_loss(model_weights, exclude_bias = False, policy_weights = None) -> float:
    critic_loss, actor_loss = 0.0, 0.0
    for weights in model_weights:
        if not (exclude_bias and "bias" in weights.name):
            critic_loss += tf.nn.l2_loss(weights)

    if policy_weights is not None:
        for weights in policy_weights:
            if not (exclude_bias and "bias" in weights.name):
                actor_loss += tf.nn.l2_loss(weights)

    return critic_loss + actor_loss

def compute_vib_loss(encoding, encoding_size) -> float:
    prior = tf.distributions.Normal(np.zeros(encoding_size, dtype=np.float32), np.ones(encoding_size, dtype=np.float32))
    info_loss = tf.reduce_sum(tf.reduce_mean(tf.distributions.kl_divergence(encoding, prior), 0)) / math.log(2)
    return info_loss

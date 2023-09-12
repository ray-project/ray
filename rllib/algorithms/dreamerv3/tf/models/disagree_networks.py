"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""

from ray.rllib.algorithms.dreamerv3.tf.models.components.mlp import MLP
from ray.rllib.algorithms.dreamerv3.tf.models.components.representation_layer import (
    RepresentationLayer,
)
from ray.rllib.utils.framework import try_import_tf, try_import_tfp

_, tf, _ = try_import_tf()
tfp = try_import_tfp()


class DisagreeNetworks(tf.keras.Model):
    """Predict the RSSM's z^(t+1), given h(t), z^(t), and a(t).

    Disagreement (stddev) between the N networks in this model on what the next z^ would
    be are used to produce intrinsic rewards for enhanced, curiosity-based exploration.

    TODO
    """

    def __init__(self, *, num_networks, model_size, intrinsic_rewards_scale):
        super().__init__(name="disagree_networks")

        self.model_size = model_size
        self.num_networks = num_networks
        self.intrinsic_rewards_scale = intrinsic_rewards_scale

        self.mlps = []
        self.representation_layers = []

        for _ in range(self.num_networks):
            self.mlps.append(
                MLP(
                    model_size=self.model_size,
                    output_layer_size=None,
                    trainable=True,
                )
            )
            self.representation_layers.append(
                RepresentationLayer(model_size=self.model_size, name="disagree")
            )

    def call(self, inputs, z, a, training=None):
        return self.forward_train(a=a, h=inputs, z=z)

    def compute_intrinsic_rewards(self, h, z, a):
        forward_train_outs = self.forward_train(a=a, h=h, z=z)
        B = tf.shape(h)[0]

        # Intrinsic rewards are computed as:
        # Stddev (between the different nets) of the 32x32 discrete, stochastic
        # probabilities. Meaning that if the larger the disagreement
        # (stddev) between the nets on what the probabilities for the different
        # classes should be, the higher the intrinsic reward.
        z_predicted_probs_N_B = forward_train_outs["z_predicted_probs_N_HxB"]
        N = len(z_predicted_probs_N_B)
        z_predicted_probs_N_B = tf.stack(z_predicted_probs_N_B, axis=0)
        # Flatten z-dims (num_categoricals x num_classes).
        z_predicted_probs_N_B = tf.reshape(z_predicted_probs_N_B, shape=(N, B, -1))

        # Compute stddevs over all disagree nets (axis=0).
        # Mean over last axis ([num categoricals] x [num classes] folded axis).
        stddevs_B_mean = tf.reduce_mean(
            tf.math.reduce_std(z_predicted_probs_N_B, axis=0),
            axis=-1,
        )
        # TEST:
        stddevs_B_mean -= tf.reduce_mean(stddevs_B_mean)
        # END TEST
        return {
            "rewards_intrinsic": stddevs_B_mean * self.intrinsic_rewards_scale,
            "forward_train_outs": forward_train_outs,
        }

    def forward_train(self, a, h, z):
        HxB = tf.shape(h)[0]
        # Fold z-dims.
        z = tf.reshape(z, shape=(HxB, -1))
        # Concat all input components (h, z, and a).
        inputs_ = tf.stop_gradient(tf.concat([h, z, a], axis=-1))

        z_predicted_probs_N_HxB = [
            repr(mlp(inputs_))[1]  # [0]=sample; [1]=returned probs
            for mlp, repr in zip(self.mlps, self.representation_layers)
        ]
        # shape=(N, HxB, [num categoricals], [num classes]); N=number of disagree nets.
        # HxB -> folded horizon_H x batch_size_B (from dreamed data).

        return {"z_predicted_probs_N_HxB": z_predicted_probs_N_HxB}

"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

from ray.rllib.algorithms.dreamerv3.tf.models.components.mlp import MLP
from ray.rllib.algorithms.dreamerv3.tf.models.components.reward_predictor_layer import (
    RewardPredictorLayer,
)
from ray.rllib.algorithms.dreamerv3.utils import (
    get_gru_units,
    get_num_z_categoricals,
    get_num_z_classes,
)
from ray.rllib.utils.framework import try_import_tf

_, tf, _ = try_import_tf()


class RewardPredictor(tf.keras.Model):
    """Wrapper of MLP and RewardPredictorLayer to predict rewards for the world model.

    Predicted rewards are used to produce "dream data" to learn the policy in.
    """

    def __init__(
        self,
        *,
        model_size: Optional[str] = "XS",
        num_buckets: int = 255,
        lower_bound: float = -20.0,
        upper_bound: float = 20.0,
    ):
        """Initializes a RewardPredictor instance.

        Args:
            model_size: The "Model Size" used according to [1] Appendinx B.
                Determines the exact size of the underlying MLP.
            num_buckets: The number of buckets to create. Note that the number of
                possible symlog'd outcomes from the used distribution is
                `num_buckets` + 1:
                lower_bound --bucket-- o[1] --bucket-- o[2] ... --bucket-- upper_bound
                o=outcomes
                lower_bound=o[0]
                upper_bound=o[num_buckets]
            lower_bound: The symlog'd lower bound for a possible reward value.
                Note that a value of -20.0 here already allows individual (actual env)
                rewards to be as low as -400M. Buckets will be created between
                `lower_bound` and `upper_bound`.
            upper_bound: The symlog'd upper bound for a possible reward value.
                Note that a value of +20.0 here already allows individual (actual env)
                rewards to be as high as 400M. Buckets will be created between
                `lower_bound` and `upper_bound`.
        """
        super().__init__(name="reward_predictor")
        self.model_size = model_size

        self.mlp = MLP(
            model_size=model_size,
            output_layer_size=None,
        )
        self.reward_layer = RewardPredictorLayer(
            num_buckets=num_buckets,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
        )

        # Trace self.call.
        dl_type = tf.keras.mixed_precision.global_policy().compute_dtype or tf.float32
        self.call = tf.function(
            input_signature=[
                tf.TensorSpec(shape=[None, get_gru_units(model_size)], dtype=dl_type),
                tf.TensorSpec(
                    shape=[
                        None,
                        get_num_z_categoricals(model_size),
                        get_num_z_classes(model_size),
                    ],
                    dtype=dl_type,
                ),
            ]
        )(self.call)

    def call(self, h, z):
        """Computes the expected reward using N equal sized buckets of possible values.

        Args:
            h: The deterministic hidden state of the sequence model. [B, dim(h)].
            z: The stochastic discrete representations of the original
                observation input. [B, num_categoricals, num_classes].
        """
        # Flatten last two dims of z.
        assert len(z.shape) == 3
        z_shape = tf.shape(z)
        z = tf.reshape(z, shape=(z_shape[0], -1))
        assert len(z.shape) == 2
        out = tf.concat([h, z], axis=-1)
        out.set_shape(
            [
                None,
                (
                    get_num_z_categoricals(self.model_size)
                    * get_num_z_classes(self.model_size)
                    + get_gru_units(self.model_size)
                ),
            ]
        )
        # Send h-cat-z through MLP.
        out = self.mlp(out)
        # Return a) mean reward OR b) a tuple: (mean reward, logits over the reward
        # buckets).
        return self.reward_layer(out)

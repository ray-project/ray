"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Optional

from ray.rllib.algorithms.dreamerv3.utils import (
    get_num_z_categoricals,
    get_num_z_classes,
)
from ray.rllib.utils.framework import try_import_tf, try_import_tfp

_, tf, _ = try_import_tf()
tfp = try_import_tfp()


class RepresentationLayer(tf.keras.layers.Layer):
    """A representation (z-state) generating layer.

    The value for z is the result of sampling from a categorical distribution with
    shape B x `num_classes`. So a computed z-state consists of `num_categoricals`
    one-hot vectors, each of size `num_classes_per_categorical`.
    """

    def __init__(
        self,
        *,
        model_size: Optional[str] = "XS",
        num_categoricals: Optional[int] = None,
        num_classes_per_categorical: Optional[int] = None,
    ):
        """Initializes a RepresentationLayer instance.

        Args:
            model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different parameters.
            num_categoricals: Overrides the number of categoricals used in the z-states.
                In [1], 32 is used for any model size.
            num_classes_per_categorical: Overrides the number of classes within each
                categorical used for the z-states. In [1], 32 is used for any model
                dimension.
        """
        self.num_categoricals = get_num_z_categoricals(
            model_size, override=num_categoricals
        )
        self.num_classes_per_categorical = get_num_z_classes(
            model_size, override=num_classes_per_categorical
        )

        super().__init__(
            name=f"z{self.num_categoricals}x{self.num_classes_per_categorical}"
        )

        self.z_generating_layer = tf.keras.layers.Dense(
            self.num_categoricals * self.num_classes_per_categorical,
            activation=None,
        )

    def call(self, inputs):
        """Produces a discrete, differentiable z-sample from some 1D input tensor.

        Pushes the input_ tensor through our dense layer, which outputs
        32(B=num categoricals)*32(c=num classes) logits. Logits are used to:

        1) sample stochastically
        2) compute probs (via softmax)
        3) make sure the sampling step is differentiable (see [2] Algorithm 1):
            sample=one_hot(draw(logits))
            probs=softmax(logits)
            sample=sample + probs - stop_grad(probs)
            -> Now sample has the gradients of the probs.

        Args:
            inputs: The input to our z-generating layer. This might be a) the combined
                (concatenated) outputs of the (image?) encoder + the last hidden
                deterministic state, or b) the output of the dynamics predictor MLP
                network.

        Returns:
            Tuple consisting of a differentiable z-sample and the probabilities for the
            categorical distribution (in the shape of [B, num_categoricals,
            num_classes]) that created this sample.
        """
        # Compute the logits (no activation) for our `num_categoricals` Categorical
        # distributions (with `num_classes_per_categorical` classes each).
        logits = self.z_generating_layer(inputs)
        # Reshape the logits to [B, num_categoricals, num_classes]
        logits = tf.reshape(
            logits,
            shape=(-1, self.num_categoricals, self.num_classes_per_categorical),
        )
        # Compute the probs (based on logits) via softmax.
        probs = tf.nn.softmax(tf.cast(logits, tf.float32))
        # Add the unimix weighting (1% uniform) to the probs.
        # See [1]: "Unimix categoricals: We parameterize the categorical distributions
        # for the world model representations and dynamics, as well as for the actor
        # network, as mixtures of 1% uniform and 99% neural network output to ensure
        # a minimal amount of probability mass on every class and thus keep log
        # probabilities and KL divergences well behaved."
        probs = 0.99 * probs + 0.01 * (1.0 / self.num_classes_per_categorical)

        # Danijar's code does: distr = [Distr class](logits=tf.log(probs)).
        # Not sure why we don't directly use the already available probs instead.
        logits = tf.math.log(probs)

        # Create the distribution object using the unimix'd logits.
        distribution = tfp.distributions.Independent(
            tfp.distributions.OneHotCategorical(logits=logits),
            reinterpreted_batch_ndims=1,
        )

        # Draw a one-hot sample (B, num_categoricals, num_classes).
        sample = tf.cast(distribution.sample(), tf.float32)
        # Make sure we can take gradients "straight-through" the sampling step
        # by adding the probs and subtracting the sg(probs). Note that `sample`
        # does not have any gradients as it's the result of a Categorical sample step,
        # which is non-differentiable (other than say a Gaussian sample step).
        # [1] "The representations are sampled from a vector of softmax distributions
        # and we take straight-through gradients through the sampling step."
        # [2] Algorithm 1.
        differentiable_sample = tf.cast(
            (tf.stop_gradient(sample) + probs - tf.stop_gradient(probs)),
            tf.keras.mixed_precision.global_policy().compute_dtype or tf.float32,
        )
        return differentiable_sample, probs

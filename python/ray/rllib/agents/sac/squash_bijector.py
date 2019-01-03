import tensorflow as tf
import tensorflow_probability as tfp


class SquashBijector(tfp.bijectors.Tanh):
    """Bijector similar to Tanh-bijector, but with stable log det jacobian."""

    def _forward_log_det_jacobian(self, x):
        return 2.0 * (
            tf.log(tf.constant(2.0, dtype=x.dtype))
            - x
            - tf.nn.softplus(-2.0 * x))

    def _inverse_log_det_jacobian(self, *args, **kwargs):
        return tfp.bijectors.Bijector._inverse_log_det_jacobian(
            self, *args, **kwargs)

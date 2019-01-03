from collections import OrderedDict

import tensorflow as tf
import tensorflow_probability as tfp

from .squash_bijector import SquashBijector


SCALE_DIAG_MIN_MAX = (-20, 2)


class GaussianLatentSpacePolicy(object):
    def __init__(self,
                 input_shape,
                 output_shape,
                 hidden_layer_sizes,
                 squash=True,
                 activation='relu',
                 output_activation='linear',
                 name=None,
                 *args,
                 **kwargs):
        self._squash = squash

        self.condition_inputs = [tf.keras.layers.Input(shape=input_shape)]

        conditions = (
            tf.keras.layers.Concatenate(axis=-1)(self.condition_inputs)
            if len(self.condition_inputs) > 1
            else self.condition_inputs[0])

        out = conditions
        for units in hidden_layer_sizes:
            out = tf.keras.layers.Dense(
                units, *args, activation=activation, **kwargs)(out)

        out = tf.keras.layers.Dense(
            output_shape[0] * 2, *args,
            activation=output_activation, **kwargs
        )(out)

        shift, log_scale_diag = tf.keras.layers.Lambda(
            lambda shift_and_log_scale_diag: tf.split(
                shift_and_log_scale_diag,
                num_or_size_splits=2,
                axis=-1)
        )(out)

        log_scale_diag = tf.keras.layers.Lambda(
            lambda log_scale_diag: tf.clip_by_value(
                log_scale_diag, *SCALE_DIAG_MIN_MAX)
        )(log_scale_diag)

        batch_size = tf.keras.layers.Lambda(
            lambda x: tf.shape(x)[0])(conditions)

        base_distribution = tfp.distributions.MultivariateNormalDiag(
            loc=tf.zeros(output_shape),
            scale_diag=tf.ones(output_shape))

        latents = tf.keras.layers.Lambda(
            lambda batch_size: base_distribution.sample(batch_size)
        )(batch_size)

        def raw_actions_fn(inputs):
            shift, log_scale_diag, latents = inputs
            bijector = tfp.bijectors.Affine(
                shift=shift,
                scale_diag=tf.exp(log_scale_diag))
            actions = bijector.forward(latents)
            return actions

        raw_actions = tf.keras.layers.Lambda(
            raw_actions_fn
        )((shift, log_scale_diag, latents))

        squash_bijector = (
            SquashBijector()
            if self._squash
            else tfp.bijectors.Identity())

        actions = tf.keras.layers.Lambda(
            lambda raw_actions: squash_bijector.forward(raw_actions)
        )(raw_actions)

        self.actions_model = tf.keras.Model(self.condition_inputs, actions)

        def log_pis_fn(inputs):
            shift, log_scale_diag, actions = inputs
            base_distribution = tfp.distributions.MultivariateNormalDiag(
                loc=tf.zeros(output_shape),
                scale_diag=tf.ones(output_shape))
            bijector = tfp.bijectors.Chain((
                squash_bijector,
                tfp.bijectors.Affine(
                    shift=shift,
                    scale_diag=tf.exp(log_scale_diag)),
            ))
            distribution = (
                tfp.distributions.ConditionalTransformedDistribution(
                    distribution=base_distribution,
                    bijector=bijector))

            log_pis = distribution.log_prob(actions)[:, None]
            return log_pis

        self.actions_input = tf.keras.layers.Input(shape=output_shape)

        log_pis = tf.keras.layers.Lambda(
            log_pis_fn)([shift, log_scale_diag, self.actions_input])

        self.log_pis_model = tf.keras.Model(
            (*self.condition_inputs, self.actions_input), log_pis)

        # self.diagnostics_model = tf.keras.Model(
        #     self.condition_inputs,
        #     (shift, log_scale_diag, log_pis, raw_actions, actions))

    @property
    def trainable_variables(self):
        return self.actions_model.trainable_variables

    def reset(self):
        pass

    def actions(self, conditions):
        return self.actions_model(conditions)

    def log_pis(self, conditions, actions):
        return self.log_pis_model([*conditions, actions])

    def actions_np(self, conditions):
        return self.actions_model.predict(conditions)

    def log_pis_np(self, conditions, actions):
        return self.log_pis_model.predict([*conditions, actions])

    def get_weights(self, *args, **kwargs):
        return self.actions_model.get_weights(*args, **kwargs)

    def set_weights(self, *args, **kwargs):
        return self.actions_model.set_weights(*args, **kwargs)

    def get_diagnostics(self, iteration, batch):
        """Return diagnostic information of the policy.

        Returns the mean, min, max, and standard deviation of means and
        covariances.
        """
        return OrderedDict({})


def feedforward_model(input_shape,
                      hidden_layer_sizes,
                      output_size,
                      activation='relu',
                      output_activation='linear',
                      name=None,
                      *args,
                      **kwargs):
    input_ = tf.keras.layers.Input(shape=input_shape)

    concatenated_input = tf.keras.layers.Lambda(
        lambda x: tf.concat(x, axis=-1)
    )(input_)

    out = concatenated_input
    for units in hidden_layer_sizes:
        out = tf.keras.layers.Dense(
            units, *args, activation=activation, **kwargs
        )(out)

    output = tf.keras.layers.Dense(
        output_size, *args, activation=output_activation, **kwargs
    )(out)

    model = tf.keras.Model(input_, output)

    return model

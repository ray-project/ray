from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from rllib.models.tf import attention
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


def bit_shift_generator(seq_length, shift, batch_size):
    while True:
        values = np.array([0., 1.], dtype=np.float32)
        seq = np.random.choice(values, (batch_size, seq_length, 1))
        targets = np.squeeze(np.roll(seq, shift, axis=1).astype(np.int32))
        targets[:, :shift] = 0
        yield seq, targets


def make_model(seq_length, num_tokens, num_layers, attn_dim, num_heads,
               head_dim, ff_hidden_dim):

    return tf.keras.Sequential((
        attention.make_TrXL(seq_length, num_layers, attn_dim, num_heads,
                            head_dim, ff_hidden_dim),
        tf.keras.layers.Dense(num_tokens),
    ))


def train_loss(targets, outputs):
    loss = tf.nn.sparse_softmax_cross_entropy_with_logits(
        labels=targets, logits=outputs)
    return tf.reduce_mean(loss)


def train_bit_shift(seq_length, num_iterations, print_every_n):

    optimizer = tf.keras.optimizers.Adam(1e-3)

    model = make_model(
        seq_length,
        num_tokens=2,
        num_layers=1,
        attn_dim=10,
        num_heads=5,
        head_dim=20,
        ff_hidden_dim=20,
    )

    shift = 10
    train_batch = 10
    test_batch = 100
    data_gen = bit_shift_generator(
        seq_length, shift=shift, batch_size=train_batch)
    test_gen = bit_shift_generator(
        seq_length, shift=shift, batch_size=test_batch)

    @tf.function
    def update_step(inputs, targets):
        loss_fn = lambda: train_loss(targets, model(inputs))
        var_fn = lambda: model.trainable_variables
        optimizer.minimize(loss_fn, var_fn)

    for i, (inputs, targets) in zip(range(num_iterations), data_gen):
        update_step(
            tf.convert_to_tensor(inputs), tf.convert_to_tensor(targets))

        if i % print_every_n == 0:
            test_inputs, test_targets = next(test_gen)
            print(i, train_loss(test_targets, model(test_inputs)))


if __name__ == "__main__":
    tf.enable_eager_execution()
    train_bit_shift(
        seq_length=20,
        num_iterations=2000,
        print_every_n=200,
    )

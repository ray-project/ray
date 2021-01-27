from gym.spaces import Box, Discrete
import numpy as np

from rllib.models.tf.attention_net import TrXLNet
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


def bit_shift_generator(seq_length, shift, batch_size):
    while True:
        values = np.array([0., 1.], dtype=np.float32)
        seq = np.random.choice(values, (batch_size, seq_length, 1))
        targets = np.squeeze(np.roll(seq, shift, axis=1).astype(np.int32))
        targets[:, :shift] = 0
        yield seq, targets


def train_loss(targets, outputs):
    loss = tf.nn.sparse_softmax_cross_entropy_with_logits(
        labels=targets, logits=outputs)
    return tf.reduce_mean(loss)


def train_bit_shift(seq_length, num_iterations, print_every_n):

    optimizer = tf.keras.optimizers.Adam(1e-3)

    model = TrXLNet(
        observation_space=Box(low=0, high=1, shape=(1, ), dtype=np.int32),
        action_space=Discrete(2),
        num_outputs=2,
        model_config={"max_seq_len": seq_length},
        name="trxl",
        num_transformer_units=1,
        attention_dim=10,
        num_heads=5,
        head_dim=20,
        position_wise_mlp_dim=20,
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
        model_out = model(
            {
                "obs": inputs
            },
            state=[tf.reshape(inputs, [-1, seq_length, 1])],
            seq_lens=np.full(shape=(train_batch, ), fill_value=seq_length))
        optimizer.minimize(lambda: train_loss(targets, model_out),
                           lambda: model.trainable_variables)

    for i, (inputs, targets) in zip(range(num_iterations), data_gen):
        inputs_in = np.reshape(inputs, [-1, 1])
        targets_in = np.reshape(targets, [-1])
        update_step(
            tf.convert_to_tensor(inputs_in), tf.convert_to_tensor(targets_in))

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

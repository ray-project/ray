import numpy as np

from ray.rllib.policy.rnn_sequencing import chop_into_sequences
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_torch


def get_filter_config(shape):
    """Returns a default Conv2D filter config (list) for a given image shape.

    Args:
        shape (Tuple[int]): The input (image) shape, e.g. (84,84,3).

    Returns:
        List[list]: The Conv2D filter configuration usable as `conv_filters`
            inside a model config dict.
    """
    shape = list(shape)
    filters_84x84 = [
        [16, [8, 8], 4],
        [32, [4, 4], 2],
        [256, [11, 11], 1],
    ]
    filters_42x42 = [
        [16, [4, 4], 2],
        [32, [4, 4], 2],
        [256, [11, 11], 1],
    ]
    if len(shape) == 3 and shape[:2] == [84, 84]:
        return filters_84x84
    elif len(shape) == 3 and shape[:2] == [42, 42]:
        return filters_42x42
    else:
        raise ValueError(
            "No default configuration for obs shape {}".format(shape) +
            ", you must specify `conv_filters` manually as a model option. "
            "Default configurations are only available for inputs of shape "
            "[42, 42, K] and [84, 84, K]. You may alternatively want "
            "to use a custom model or preprocessor.")


def get_initializer(name, framework="tf"):
    """Returns a framework specific initializer, given a name string.

    Args:
        name (str): One of "xavier_uniform" (default), "xavier_normal".
        framework (str): One of "tf" or "torch".

    Returns:
        A framework-specific initializer function, e.g.
            tf.keras.initializers.GlorotUniform or
            torch.nn.init.xavier_uniform_.

    Raises:
        ValueError: If name is an unknown initializer.
    """
    if framework == "torch":
        _, nn = try_import_torch()
        if name in [None, "default", "xavier_uniform"]:
            return nn.init.xavier_uniform_
        elif name == "xavier_normal":
            return nn.init.xavier_normal_
    else:
        tf1, tf, tfv = try_import_tf()
        if name in [None, "default", "xavier_uniform"]:
            return tf.keras.initializers.GlorotUniform
        elif name == "xavier_normal":
            return tf.keras.initializers.GlorotNormal

    raise ValueError("Unknown activation ({}) for framework={}!".format(
        name, framework))


def rnn_preprocess_train_batch(train_batch, max_seq_len):
    assert "state_in_0" in train_batch
    state_keys = []
    feature_keys_ = []
    for k, v in train_batch.items():
        if k.startswith("state_in_"):
            state_keys.append(k)
        elif not k.startswith("state_out_") and k != "infos" and \
                isinstance(v, np.ndarray):
            feature_keys_.append(k)

    states_already_reduced_to_init = \
        len(train_batch["state_in_0"]) < len(train_batch["obs"])

    feature_sequences, initial_states, seq_lens = \
        chop_into_sequences(
            feature_columns=[train_batch[k] for k in feature_keys_],
            state_columns=[train_batch[k] for k in state_keys],
            max_seq_len=max_seq_len,
            episode_ids=train_batch.get(SampleBatch.EPS_ID),
            unroll_ids=train_batch.get(SampleBatch.UNROLL_ID),
            agent_indices=train_batch.get(SampleBatch.AGENT_INDEX),
            dynamic_max=True,
            shuffle=False,
            seq_lens=getattr(train_batch, "seq_lens", train_batch.get("seq_lens")),
            states_already_reduced_to_init=states_already_reduced_to_init,
        )
    for i, k in enumerate(feature_keys_):
        train_batch[k] = feature_sequences[i]
    for i, k in enumerate(state_keys):
        train_batch[k] = initial_states[i]
    train_batch["seq_lens"] = seq_lens
    return train_batch


def attention_preprocess_train_batch(train_batch, max_seq_len):
    TODO: check with tf version rn
    # Should be the same as for RecurrentNets, but with dynamic-max=False.
    assert "state_in_0" in train_batch
    state_keys = []
    feature_keys_ = []
    for k, v in train_batch.items():
        if k.startswith("state_in_"):
            state_keys.append(k)
        elif not k.startswith(
                "state_out_"
        ) and k != "infos" and k != "seq_lens" and isinstance(v, np.ndarray):
            feature_keys_.append(k)

    feature_sequences, initial_states, seq_lens = \
        chop_into_sequences(
            episode_ids=None,
            unroll_ids=None,
            agent_indices=None,
            feature_columns=[train_batch[k] for k in feature_keys_],
            state_columns=[train_batch[k] for k in state_keys],
            max_seq_len=max_seq_len,
            dynamic_max=False,
            seq_lens=train_batch.seq_lens,
            shuffle=False)
    for i, k in enumerate(feature_keys_):
        train_batch[k] = feature_sequences[i]
    for i, k in enumerate(state_keys):
        train_batch[k] = initial_states[i]
    train_batch["seq_lens"] = np.array(seq_lens)
    return train_batch

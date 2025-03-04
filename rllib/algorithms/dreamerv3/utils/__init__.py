"""
Utility functions for the DreamerV3 ([1]) algorithm.

[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""

_ALLOWED_MODEL_DIMS = [
    # RLlib debug sizes (not mentioned in [1]).
    "nano",
    "micro",
    "mini",
    "XXS",
    # Regular sizes (listed in table B in [1]).
    "XS",
    "S",
    "M",
    "L",
    "XL",
]


def get_cnn_multiplier(model_size, override=None):
    if override is not None:
        return override

    assert model_size in _ALLOWED_MODEL_DIMS
    cnn_multipliers = {
        "nano": 2,
        "micro": 4,
        "mini": 8,
        "XXS": 16,
        "XS": 24,
        "S": 32,
        "M": 48,
        "L": 64,
        "XL": 96,
    }
    return cnn_multipliers[model_size]


def get_dense_hidden_units(model_size, override=None):
    if override is not None:
        return override

    assert model_size in _ALLOWED_MODEL_DIMS
    dense_units = {
        "nano": 16,
        "micro": 32,
        "mini": 64,
        "XXS": 128,
        "XS": 256,
        "S": 512,
        "M": 640,
        "L": 768,
        "XL": 1024,
    }
    return dense_units[model_size]


def get_gru_units(model_size, override=None):
    if override is not None:
        return override

    assert model_size in _ALLOWED_MODEL_DIMS
    gru_units = {
        "nano": 16,
        "micro": 32,
        "mini": 64,
        "XXS": 128,
        "XS": 256,
        "S": 512,
        "M": 1024,
        "L": 2048,
        "XL": 4096,
    }
    return gru_units[model_size]


def get_num_z_categoricals(model_size, override=None):
    if override is not None:
        return override

    assert model_size in _ALLOWED_MODEL_DIMS
    gru_units = {
        "nano": 4,
        "micro": 8,
        "mini": 16,
        "XXS": 32,
        "XS": 32,
        "S": 32,
        "M": 32,
        "L": 32,
        "XL": 32,
    }
    return gru_units[model_size]


def get_num_z_classes(model_size, override=None):
    if override is not None:
        return override

    assert model_size in _ALLOWED_MODEL_DIMS
    gru_units = {
        "nano": 4,
        "micro": 8,
        "mini": 16,
        "XXS": 32,
        "XS": 32,
        "S": 32,
        "M": 32,
        "L": 32,
        "XL": 32,
    }
    return gru_units[model_size]


def get_num_curiosity_nets(model_size, override=None):
    if override is not None:
        return override

    assert model_size in _ALLOWED_MODEL_DIMS
    num_curiosity_nets = {
        "nano": 8,
        "micro": 8,
        "mini": 8,
        "XXS": 8,
        "XS": 8,
        "S": 8,
        "M": 8,
        "L": 8,
        "XL": 8,
    }
    return num_curiosity_nets[model_size]


def get_num_dense_layers(model_size, override=None):
    if override is not None:
        return override

    assert model_size in _ALLOWED_MODEL_DIMS
    num_dense_layers = {
        "nano": 1,
        "micro": 1,
        "mini": 1,
        "XXS": 1,
        "XS": 1,
        "S": 2,
        "M": 3,
        "L": 4,
        "XL": 5,
    }
    return num_dense_layers[model_size]


def do_symlog_obs(observation_space, symlog_obs_user_setting):
    # If our symlog_obs setting is NOT set specifically (it's set to "auto"), return
    # True if we don't have an image observation space, otherwise return False.

    # TODO (sven): Support mixed observation spaces.

    is_image_space = len(observation_space.shape) in [2, 3]
    return (
        not is_image_space
        if symlog_obs_user_setting == "auto"
        else symlog_obs_user_setting
    )

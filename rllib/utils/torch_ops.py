from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


def sequence_mask(lengths, maxlen, dtype=None):
    """
    Exact same behavior as tf.sequence_mask.
    Thanks to Dimitris Papatheodorou
    (https://discuss.pytorch.org/t/pytorch-equivalent-for-tf-sequence-mask/
    39036).
    """
    if maxlen is None:
        maxlen = lengths.max()

    mask = ~(torch.ones((len(lengths), maxlen)).cumsum(dim=1).t() > lengths). \
        t()
    mask.type(dtype or torch.bool)

    return mask


def convert_to_non_torch_type(stats_dict):
    """Converts values in stats_dict to non-Tensor numpy or python types.

    Args:
        stats_dict (dict): A flat key, value dict, the values of which will be
            converted and returned as a new dict.

    Returns:
        dict: A new dict with the same structure as stats_dict, but with all
            values converted to non-torch Tensor types.
    """
    ret = {}
    for k, v in stats_dict.items():
        if isinstance(v, torch.Tensor):
            ret[k] = v.cpu().item() if len(v.size()) == 0 else v.cpu().numpy()
        else:
            ret[k] = v
    return ret

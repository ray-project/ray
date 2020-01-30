from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


def sequence_mask(lengths, maxlen, dtype=torch.bool):
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
    mask.type(dtype)

    return mask

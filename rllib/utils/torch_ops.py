import logging

from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()

logger = logging.getLogger(__name__)

try:
    import tree
except (ImportError, ModuleNotFoundError) as e:
    logger.warning("`dm-tree` is not installed! Run `pip install dm-tree`.")
    raise e


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


def convert_to_non_torch_type(stats):
    """Converts values in stats_dict to non-Tensor numpy or python types.

    Args:
        stats (any): Any (possibly nested) struct, the values in which will be
            converted and returned as a new struct with all torch tensors
            being converted to numpy types.

    Returns:
        dict: A new dict with the same structure as stats_dict, but with all
            values converted to non-torch Tensor types.
    """

    # The mapping function used to numpyize torch Tensors.
    def mapping(item):
        if isinstance(item, torch.Tensor):
            return item.cpu().item() if len(item.size()) == 0 else \
                item.cpu().numpy()
        else:
            return item

    return tree.map_structure(mapping, stats)

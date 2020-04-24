import logging

from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def make_time_major(policy, seq_lens, tensor, drop_last=False):
    """Swaps batch and trajectory axis.

    Arguments:
        policy: Policy reference
        seq_lens: Sequence lengths if recurrent or None
        tensor: A tensor or list of tensors to reshape.
        drop_last: A bool indicating whether to drop the last
        trajectory item.

    Returns:
        res: A tensor with swapped axes or a list of tensors with
        swapped axes.
    """
    if isinstance(tensor, (list, tuple)):
        return [
            make_time_major(policy, seq_lens, t, drop_last) for t in tensor
        ]

    if policy.is_recurrent():
        B = seq_lens.shape[0]
        T = tensor.shape[0] // B
    else:
        # Important: chop the tensor into batches at known episode cut
        # boundaries. TODO(ekl) this is kind of a hack
        T = policy.config["rollout_fragment_length"]
        B = tensor.shape[0] // T
    rs = torch.reshape(tensor, [B, T] + list(tensor.shape[1:]))

    # Swap B and T axes.
    res = torch.transpose(rs, 1, 0)

    if drop_last:
        return res[:-1]
    return res


def choose_optimizer(policy, config):
    if policy.config["opt_type"] == "adam":
        return torch.optim.Adam(
            params=policy.model.parameters(), lr=policy.cur_lr)
    else:
        return torch.optim.RMSProp(
            params=policy.model.parameters(),
            lr=policy.cur_lr,
            weight_decay=config["decay"],
            momentum=config["momentum"],
            eps=config["epsilon"])

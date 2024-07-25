import copy

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import NetworkType
from ray.util import PublicAPI


torch, _ = try_import_torch()


def make_target_network(main_net: NetworkType) -> NetworkType:
    """Creates a (deep) copy of `main_net` (including synched weights) and returns it.

    Args:
        main_net: The main network to return a target network for

    Returns:
        The copy of `main_net` that can be used as a target net. Note that the weights
        of the returned net are already synched (identical) with `main_net`.
    """
    # Deepcopy the main net (this should already take care of synching all weights).
    target_net = copy.deepcopy(main_net)
    # Make the target net not trainable.
    if isinstance(main_net, torch.nn.Module):
        target_net.requires_grad_(False)
    else:
        raise ValueError(f"Unsupported framework for given `main_net` {main_net}!")

    return target_net


@PublicAPI(stability="beta")
def update_target_network(
    *,
    main_net: NetworkType,
    target_net: NetworkType,
    tau: float,
) -> None:
    """Updates a target network (from a "main" network) using Polyak averaging.

    Thereby:
    new_target_net_weight = (
        tau * main_net_weight + (1.0 - tau) * current_target_net_weight
    )

    Args:
        main_net: The nn.Module to update from.
        target_net: The target network to update.
        tau: The tau value to use in the Polyak averaging formula. Use 1.0 for a
            complete sync of the weights (target and main net will be the exact same
            after updating).
    """
    if isinstance(main_net, torch.nn.Module):
        from ray.rllib.utils.torch_utils import update_target_network as _update_target

    else:
        raise ValueError(f"Unsupported framework for given `main_net` {main_net}!")

    _update_target(main_net=main_net, target_net=target_net, tau=tau)

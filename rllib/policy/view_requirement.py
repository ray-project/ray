import gym
import numpy as np
from typing import List, Optional, Union, TYPE_CHECKING

from ray.rllib.utils.framework import try_import_torch

if TYPE_CHECKING:
    from ray.rllib.policy.policy import Policy

torch, _ = try_import_torch()


class ViewRequirement:
    """Single view requirement (for one column in an SampleBatch/input_dict).

    Note: This is an experimental class used only if
    `_use_trajectory_view_api` in the config is set to True.

    Policies and ModelV2s return a Dict[str, ViewRequirement] upon calling
    their `[train|inference]_view_requirements()` methods, where the str key
    represents the column name (C) under which the view is available in the
    input_dict/SampleBatch and ViewRequirement specifies the actual underlying
    column names (in the original data buffer), timestep shifts, and other
    options to build the view.

    Examples:
        >>> # The default ViewRequirement for a Model is:
        >>> req = [ModelV2].inference_view_requirements
        >>> print(req)
        {"obs": ViewRequirement(shift=0)}
    """

    def __init__(self,
                 data_col: Optional[str] = None,
                 space: gym.Space = None,
                 shift: Union[int, str, List[int]] = 0,
                 used_for_training: bool = True):
        """Initializes a ViewRequirement object.

        Args:
            data_col (): The data column name from the SampleBatch (str key).
                If None, use the dict key under which this ViewRequirement
                resides.
            space (gym.Space): The gym Space used in case we need to pad data
                in inaccessible areas of the trajectory (t<0 or t>H).
                Default: Simple box space, e.g. rewards.
            shift (Union[int, str, List[int]]): Single shift value of list of
                shift values to use relative to the underlying `data_col`.
                Example: For a view column "prev_actions", you can set
                `data_col="actions"` and `shift=-1`.
                Example: For a view column "obs" in an Atari framestacking
                fashion, you can set `data_col="obs"` and
                `shift=[-3, -2, -1, 0]`.
                Example: For the obs input to an attention net, you can specify
                a range via a str: `shift="-100:0"`, which will pass in the
                past 100 observations plus the current one.
            used_for_training (bool): Whether the data will be used for
                training. If False, the column will not be copied into the
                final train batch.
        """
        self.data_col = data_col
        self.space = space or gym.spaces.Box(
            float("-inf"), float("inf"), shape=())

        self.shift = shift
        # Special case: Providing a (probably larger) range of indices, e.g.
        # "-100:0" (past 100 timesteps plus current one).
        self.shift_from = self.shift_to = None
        if isinstance(self.shift, str):
            f, t = self.shift.split(":")
            self.shift_from = int(f)
            self.shift_to = int(t)

        self.used_for_training = used_for_training

        # TODO: probably need max seq-len here + flag to indicate that only first
        #  timestep per sequence is needed (for RNNs and attention nets).
        #  also will have to provide a function (in the model?) to process
        #  train_bathes (like a 0_pad-func for RNNs)


def initialize_loss_with_dummy_batch(policy, auto=True):
    from ray.rllib.policy.sample_batch import SampleBatch

    sample_batch_size = max(policy.batch_divisibility_req, 2)
    B = 2  # For RNNs, have B=2, T=[depends on sample_batch_size]
    policy._dummy_batch = _get_dummy_batch(
        policy, batch_size=sample_batch_size)
    input_dict = policy._lazy_tensor_dict(policy._dummy_batch)
    actions, state_outs, extra_outs = \
        policy.compute_actions_from_input_dict(input_dict)
    # Add extra outs to view reqs.
    for key, value in extra_outs.items():
        policy._dummy_batch[key] = np.zeros_like(value)
    sb = SampleBatch(policy._dummy_batch)
    if state_outs:
        # TODO: (sven) This hack will not work for attention net traj.
        #  view setup.
        i = 0
        while "state_in_{}".format(i) in sb:
            sb["state_in_{}".format(i)] = sb["state_in_{}".format(i)][:B]
            if "state_out_{}".format(i) in sb:
                sb["state_out_{}".format(i)] = \
                    sb["state_out_{}".format(i)][:B]
            i += 1
    batch_for_postproc = policy._lazy_numpy_dict(sb)
    batch_for_postproc.count = sb.count
    postprocessed_batch = policy.postprocess_trajectory(batch_for_postproc)
    if state_outs:
        seq_len = (policy.batch_divisibility_req // B) or 1
        postprocessed_batch["seq_lens"] = \
            np.array([seq_len for _ in range(B)], dtype=np.int32)
    train_batch = policy._lazy_tensor_dict(postprocessed_batch)
    if policy._loss is not None:
        policy._loss(policy, policy.model, policy.dist_class, train_batch)

    # Add new columns automatically to view-reqs.
    if policy.config["_use_trajectory_view_api"] and auto:
        # Add those needed for postprocessing and training.
        all_accessed_keys = train_batch.accessed_keys | batch_for_postproc.accessed_keys | batch_for_postproc.added_keys
        for key in all_accessed_keys:
            if key not in policy.view_requirements:
                policy.view_requirements[key] = ViewRequirement()
        if policy._loss:
            # Tag those only needed for post-processing.
            for key in batch_for_postproc.accessed_keys:
                if key not in train_batch.accessed_keys:
                    policy.view_requirements[key].used_for_training = False
            # Remove those not needed at all (leave those that are needed
            # by Sampler to properly execute sample collection).
            for key in list(policy.view_requirements.keys()):
                if key not in all_accessed_keys and key not in [
                    SampleBatch.EPS_ID, SampleBatch.AGENT_INDEX,
                    SampleBatch.UNROLL_ID, SampleBatch.DONES] and \
                        key not in policy.model.inference_view_requirements:
                    del policy.view_requirements[key]
        # Add those data_cols (again) that are missing and have
        # dependencies by view_cols.
        for key in list(policy.view_requirements.keys()):
            vr = policy.view_requirements[key]
            if vr.data_col is not None and vr.data_col not in policy.view_requirements:
                used_for_training = vr.data_col in train_batch.accessed_keys
                policy.view_requirements[vr.data_col] = ViewRequirement(space=vr.space, used_for_training=used_for_training)


def _get_dummy_batch(policy, batch_size=1):
    from ray.rllib.models.catalog import ModelCatalog

    # Generate a 2 batch (safer since some loss functions require at least
    # a batch size of 2).
    ret = {}
    for view_col, view_req in policy.view_requirements.items():
        if isinstance(view_req.space, (gym.spaces.Dict, gym.spaces.Tuple)):
            _, shape = ModelCatalog.get_action_shape(view_req.space)
            ret[view_col] = np.zeros((batch_size, ) + shape[1:], np.float32)
        else:
            ret[view_col] = np.zeros_like(
                [view_req.space.sample() for _ in range(batch_size)])
    return ret

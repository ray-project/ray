import gym
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


def get_default_view_requirements(policy: "Policy"):
    """Returns a default ViewRequirements dict (for backward compatibility).

    Args:
        policy (Policy): The Policy, for which to generate the ViewRequirements
            dict.

    Returns:
        ViewReqDict: The default view requirements dict.
    """
    #from ray.rllib.evaluation.postprocessing import Postprocessing
    from ray.rllib.policy.sample_batch import SampleBatch

    # Default view requirements (equal to those that we would use before
    # the trajectory view API was introduced).
    view_reqs = {
        SampleBatch.OBS: ViewRequirement(space=policy.observation_space),
        SampleBatch.NEXT_OBS: ViewRequirement(
            data_col=SampleBatch.OBS, shift=1, space=policy.observation_space),
        SampleBatch.ACTIONS: ViewRequirement(space=policy.action_space),
        SampleBatch.REWARDS: ViewRequirement(),
        SampleBatch.DONES: ViewRequirement(),
        SampleBatch.EPS_ID: ViewRequirement(),
        SampleBatch.AGENT_INDEX: ViewRequirement(),
        SampleBatch.INFOS: ViewRequirement(used_for_training=False),
        SampleBatch.ACTION_DIST_INPUTS: ViewRequirement(),
        SampleBatch.ACTION_LOGP: ViewRequirement(),
    }
    # Add the state-in/out views in case the policy has an RNN.
    if policy.is_recurrent():
        init_state = policy.get_initial_state()
        for i, s in enumerate(init_state):
            view_reqs["state_in_{}".format(i)] = ViewRequirement(
                data_col="state_out_{}".format(i),
                shift=-1,
                space=gym.spaces.Box(-1.0, 1.0, shape=s.shape))
            view_reqs["state_out_{}".format(i)] = ViewRequirement(
                space=gym.spaces.Box(-1.0, 1.0, shape=s.shape))

    return view_reqs

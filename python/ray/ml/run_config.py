from dataclasses import dataclass
from typing import List, Optional

from ray.tune.callback import Callback
from ray.util import PublicAPI


# TODO(xwjiang): Change to alpha
@dataclass
@PublicAPI(stability="beta")
class RunConfig:
    """Runtime configuration for individual trials that are run.

    This contains information that applies to individual runs of Trainable classes.
    This includes both running a Trainable by itself or running a hyperparameter
    tuning job on top of a Trainable (applies to each trial).

    Args:
        name: Name of the trial or experiment. If not provided, will be deduced
            from the Trainable.
        local_dir: Local dir to save training results to.
            Defaults to ``~/ray_results``.
        callbacks: Callbacks to invoke.
            Refer to ray.tune.callback.Callback for more info.
    """

    # TODO(xwjiang): Clarify RunConfig behavior across resume. Is one supposed to
    #  reapply some of the args here? For callbacks, how do we enforce only stateless
    #  callbacks?
    # TODO(xwjiang): Add more.
    name: Optional[str] = None
    local_dir: Optional[str] = None
    callbacks: Optional[List[Callback]] = None

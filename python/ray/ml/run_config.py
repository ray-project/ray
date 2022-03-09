from dataclasses import dataclass
from typing import List, Optional

from ray.tune.callback import Callback
from ray.util import PublicAPI


# TODO(xwjiang): Change to alpha
@dataclass
@PublicAPI(stability="beta")
class RunConfig:
    """Run time config that is shared by both Tune and Train.

    Args:
        name: Name of the run. If not provided, will be deduced
            from the trainable.
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

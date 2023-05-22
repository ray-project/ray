from dataclasses import dataclass
from typing import Any, Dict, Optional, Type, Union

from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.typing import LearningRateOrSchedule, LocalOptimizer


@ExperimentalAPI
@dataclass
class OptimizerSpec:
    """Utility spec class to make constructing Optimizers (framework specific) easier.

    Args:
        optimizer_class: Class specifier understandable by tf or torch, e.g.
            "adam", a full classpath, or the local optimizer type to use as a
            constructor.
        learning_rate: A fixed learning rate or a schedule specifier.
        ctor_kwargs: Keyword arguments to be passed to the local optimizer's
            constructor when creating it.
        grad_clip:
        grad_clip_by:
    """

    optimizer_class: Union[str, Type[LocalOptimizer]] = None
    ctor_kwargs: Dict[str, Any] = None
    # TODO: What if user does NOT want RLlib to meddle with the lr-logic at all?
    learning_rate: LearningRateOrSchedule = None
    # Gradient postprocessing (clipping) settings.
    grad_clip: Optional[float] = None
    grad_clip_by: str = None

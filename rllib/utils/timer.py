from ray.util.timer import _Timer
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.utils.timer::TimerStat",
    new="ray.util.timer::_Timer",
    error=False,
)

TimerStat = _Timer  # backwards compatibility alias

from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.metrics.window_stat import WindowStat

deprecation_warning(
    old="ray.rllib.utils.window_stat.WindowStat",
    new="ray.rllib.utils.metrics.window_stat.WindowStat",
    error=False,
)
WindowStat = WindowStat

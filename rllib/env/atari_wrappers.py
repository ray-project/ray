from ray.rllib.env.wrappers.atari_wrappers import is_atari, \
    get_wrapper_by_cls, MonitorEnv, NoopResetEnv, ClipRewardEnv, \
    FireResetEnv, EpisodicLifeEnv, MaxAndSkipEnv, WarpFrame, FrameStack, \
    ScaledFloatFrame, wrap_deepmind
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.env.atari_wrappers....",
    new="ray.rllib.env.wrappers.atari_wrappers....",
    error=False,
)

# Check whether this will get through linter.
# is_atari = is_atari
# get_wrapper_by_cls = get_wrapper_by_cls
# MonitorEnv = MonitorEnv
# NoopResetEnv = NoopResetEnv
# ClipRewardEnv = ClipRewardEnv
# FireResetEnv = FireResetEnv
# EpisodicLifeEnv = EpisodicLifeEnv
# MaxAndSkipEnv = MaxAndSkipEnv
# WarpFrame = WarpFrame
# FrameStack = FrameStack
# ScaledFloatFrame = ScaledFloatFrame
# wrap_deepmind = wrap_deepmind

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.execution.buffers",
    new="ray.rllib.utils.replay_buffers",
    help="RLlib's ReplayBuffer API has changed. Apart from the replay buffers moving, "
    "some have altered behaviour. Please refer to the docs for more information.",
    error=False,
)

from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class UnsupportedSpaceException(Exception):
    """Error for an unsupported action or observation space."""

    pass


@PublicAPI
class EnvError(Exception):
    """Error if we encounter an error during RL environment validation."""

    pass


# -------
# Error messages
# -------

# Message explaining there are no GPUs available for the
# num_gpus=n or num_gpus_per_worker=m settings.
ERR_MSG_NO_GPUS = """Found {} GPUs on your machine (GPU devices found: {})! If your machine
    does not have any GPUs, you should set the config keys `num_gpus` and
    `num_gpus_per_worker` to 0 (they may be set to 1 by default for your
    particular RL algorithm)."""

ERR_MSG_INVALID_ENV_DESCRIPTOR = """The env string you provided ('{}') is:
a) Not a supported/installed environment.
b) Not a tune-registered environment creator.
c) Not a valid env class string.

Try one of the following:
a) For Atari support: `pip install gym[atari] autorom[accept-rom-license]`.
   For VizDoom support: Install VizDoom
   (https://github.com/mwydmuch/ViZDoom/blob/master/doc/Building.md) and
   `pip install vizdoomgym`.
   For PyBullet support: `pip install pybullet`.
b) To register your custom env, do `from ray import tune;
   tune.register('[name]', lambda cfg: [return env obj from here using cfg])`.
   Then in your config, do `config['env'] = [name]`.
c) Make sure you provide a fully qualified classpath, e.g.:
   `ray.rllib.examples.env.repeat_after_me_env.RepeatAfterMeEnv`
"""

# -------
# HOWTO_ strings can be added to any error/warning/into message
# to eplain to the user, how to actually fix the encountered problem.
# -------

# HOWTO change the RLlib config, depending on how user runs the job.
HOWTO_CHANGE_CONFIG = """
To change the config for the `rllib train|rollout` command, use
  `--config={'[key]': '[value]'}` on the command line.
To change the config for `tune.run()` in a script: Modify the python dict
  passed to `tune.run(config=[...])`.
To change the config for an RLlib Algorithm instance: Modify the python dict
  passed to the Algorithm's constructor, e.g. `PPO(config=[...])`.
"""

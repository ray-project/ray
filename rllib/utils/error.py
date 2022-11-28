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
ERR_MSG_NO_GPUS = """Found {} GPUs on your machine (GPU devices found: {})! If your
    machine does not have any GPUs, you should set the config keys `num_gpus` and
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


ERR_MSG_OLD_GYM_API = """Your environment ({}) does not abide to the new gymnasium-style API!
From Ray 2.3 on, RLlib only supports the new (gym>=0.26 or gymnasium) Env APIs.
{}
Learn more about the most important changes here:
https://github.com/openai/gym and here: https://github.com/Farama-Foundation/Gymnasium

In order to fix this problem, do the following:

1) Run `pip install gymnasium` on your command line.
2) Change all your import statements in your code from
   `import gym` -> `import gymnasium as gym` OR
   `from gym.space import Discrete` -> `from gymnasium.spaces import Discrete`

For your custom (single agent) gym.Env classes:
3.1) Either wrap your old Env class via the provided `from gymnasium.wrappers import
     EnvCompatibility` wrapper class.
3.2) Alternatively to 3.1:
 - Change your `reset()` method to have the call signature 'def reset(self, *,
   seed=None, options=None)'
 - Return an additional info dict (empty dict should be fine) from your `reset()`
   method.
 - Return an additional `truncated` flag from your `step()` method (between `done` and
   `info`). This flag should indicate, whether the episode was terminated prematurely
   due to some time constraint or other kind of horizon setting.

For your custom RLlib `MultiAgentEnv` classes:
4.1) Either wrap your old MultiAgentEnv via the provided
     `from ray.rllib.env.wrappers.multi_agent_env_compatibility import
     MultiAgentEnvCompatibility` wrapper class.
4.2) Alternatively to 4.1:
 - Change your `reset()` method to have the call signature
   'def reset(self, *, seed=None, options=None)'
 - Return an additional per-agent info dict (empty dict should be fine) from your
   `reset()` method.
 - Rename `dones` into `terminateds` and only set this to True, if the episode is really
   done (as opposed to has been terminated prematurely due to some horizon/time-limit
   setting).
 - Return an additional `truncateds` per-agent dictionary flag from your `step()`
   method, including the `__all__` key (100% analogous to your `dones/terminateds`
   per-agent dict).
   Return this new `truncateds` dict between `dones/terminateds` and `infos`. This
   flag should indicate, whether the episode (for some agent or all agents) was
   terminated prematurely due to some time constraint or other kind of horizon setting.
"""


ERR_MSG_TF_POLICY_CANNOT_SAVE_KERAS_MODEL = """Could not save keras model under self[TfPolicy].model.base_model!
    This is either due to ..
    a) .. this Policy's ModelV2 not having any `base_model` (tf.keras.Model) property
    b) .. the ModelV2's `base_model` not being used by the Algorithm and thus its
       variables not being properly initialized.
"""

ERR_MSG_TORCH_POLICY_CANNOT_SAVE_MODEL = """Could not save torch model under self[TorchPolicy].model!
    This is most likely due to the fact that you are using an Algorithm that
    uses a Catalog-generated TorchModelV2 subclass, which is torch.save() cannot pickle.
"""

# -------
# HOWTO_ strings can be added to any error/warning/into message
# to eplain to the user, how to actually fix the encountered problem.
# -------

# HOWTO change the RLlib config, depending on how user runs the job.
HOWTO_CHANGE_CONFIG = """
To change the config for the `rllib train|rollout` command, use
  `--config={'[key]': '[value]'}` on the command line.
To change the config for `tune.Tuner().fit()` in a script: Modify the python dict
  passed to `tune.Tuner(param_space=[...]).fit()`.
To change the config for an RLlib Algorithm instance: Modify the python dict
  passed to the Algorithm's constructor, e.g. `PPO(config=[...])`.
"""

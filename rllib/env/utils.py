from gym import wrappers
import os
import re

from ray.rllib.env.env_context import EnvContext


def gym_env_creator(env_context: EnvContext, env_descriptor: str):
    """Tries to create a gym env given an EnvContext object and descriptor.

    Note: This function tries to construct the env from a string descriptor
    only using possibly installed RL env packages (such as gym, pybullet_envs,
    vizdoomgym, etc..). These packages are no installation requirements for
    RLlib. In case you would like to support more such env packages, add the
    necessary imports and construction logic below.

    Args:
        env_context (EnvContext): The env context object to configure the env.
            Note that this is a config dict, plus the properties:
            `worker_index`, `vector_index`, and `remote`.
        env_descriptor (str): The env descriptor, e.g. CartPole-v0,
            MsPacmanNoFrameskip-v4, VizdoomBasic-v0, or
            CartPoleContinuousBulletEnv-v0.

    Returns:
        gym.Env: The actual gym environment object.

    Raises:
        gym.error.Error: If the env cannot be constructed.
    """
    import gym
    # Allow for PyBullet or VizdoomGym envs to be used as well
    # (via string). This allows for doing things like
    # `env=CartPoleContinuousBulletEnv-v0` or
    # `env=VizdoomBasic-v0`.
    try:
        import pybullet_envs
        pybullet_envs.getList()
    except (ModuleNotFoundError, ImportError):
        pass
    try:
        import vizdoomgym
        vizdoomgym.__name__  # trick LINTer.
    except (ModuleNotFoundError, ImportError):
        pass

    # Try creating a gym env. If this fails we can output a
    # decent error message.
    try:
        return gym.make(env_descriptor, **env_context)
    except gym.error.Error:
        error_msg = f"The env string you provided ('{env_descriptor}') is:" + \
            """
a) Not a supported/installed environment.
b) Not a tune-registered environment creator.
c) Not a valid env class string.

Try one of the following:
a) For Atari support: `pip install gym[atari] atari_py`.
   For VizDoom support: Install VizDoom
   (https://github.com/mwydmuch/ViZDoom/blob/master/doc/Building.md) and
   `pip install vizdoomgym`.
   For PyBullet support: `pip install pybullet pybullet_envs`.
b) To register your custom env, do `from ray import tune;
   tune.register('[name]', lambda cfg: [return env obj from here using cfg])`.
   Then in your config, do `config['env'] = [name]`.
c) Make sure you provide a fully qualified classpath, e.g.:
   `ray.rllib.examples.env.repeat_after_me_env.RepeatAfterMeEnv`
"""
        raise gym.error.Error(error_msg)


class VideoMonitor(wrappers.Monitor):
    # Same as original method, but doesn't use the StatsRecorder as it will
    # try to add up multi-agent rewards dicts, which throws errors.
    def _after_step(self, observation, reward, done, info):
        if not self.enabled:
            return done

        # Use done["__all__"] b/c this is a multi-agent dict.
        if done["__all__"] and self.env_semantics_autoreset:
            # For envs with BlockingReset wrapping VNCEnv, this observation
            # will be the first one of the new episode
            self.reset_video_recorder()
            self.episode_id += 1
            self._flush()

        # Record video
        self.video_recorder.capture_frame()

        return done


def record_env_wrapper(env, record_env, log_dir, policy_config):
    if record_env:
        path_ = record_env if isinstance(record_env, str) else log_dir
        # Relative path: Add logdir here, otherwise, this would
        # not work for non-local workers.
        if not re.search("[/\\\]", path_):
            path_ = os.path.join(log_dir, path_)
        print(f"Setting the path for recording to {path_}")
        from ray.rllib.env.multi_agent_env import MultiAgentEnv
        wrapper_cls = VideoMonitor if isinstance(env, MultiAgentEnv) \
            else wrappers.Monitor
        env = wrapper_cls(
            env,
            path_,
            resume=True,
            force=True,
            video_callable=lambda _: True,
            mode="evaluation"
            if policy_config["in_evaluation"] else "training")
    return env

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

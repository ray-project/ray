import logging
import importlib
from typing import Union, Optional
import gymnasium as gym
import numpy as np
from ray.rllib.utils.annotations import PublicAPI


# Lazy initialization of simulation app to avoid eager initialization
_simulation_app = None
_app_launcher = None


def _get_simulation_app():
    """Lazy initialization of the Isaac Lab simulation app."""
    global _simulation_app, _app_launcher
    if _simulation_app is None:
        try:
            from isaaclab.app import AppLauncher
        except ImportError as e:
            raise ImportError(
                "Isaac Lab is not installed. Please install it using: "
                "isaacsim[all,extscache]==5.0.0"
            ) from e
        _app_launcher = AppLauncher()
        _simulation_app = _app_launcher.app
    return _simulation_app


def _get_cfg_entry_point(env_name, env_cfg_entry_point_key="env_cfg_entry_point"):
    cfg_entry_point = gym.spec(env_name).kwargs.get(env_cfg_entry_point_key)
    if isinstance(cfg_entry_point, str):
        mod_name, attr_name = cfg_entry_point.split(":")
        mod = importlib.import_module(mod_name)
        cfg_cls = getattr(mod, attr_name)
    else:
        cfg_cls = cfg_entry_point
    # load the configuration
    logging.info(f"[INFO]: Parsing configuration from: {cfg_entry_point}")
    if callable(cfg_cls):
        cfg = cfg_cls()
    else:
        cfg = cfg_cls
    return cfg

@PublicAPI
class IsaacLabEnv(gym.Env):

    def __init__(self, env_name: str, env_cfg: dict = None):
        super(IsaacLabEnv, self).__init__()
        if env_cfg is None:
            self._env_cfg = _get_cfg_entry_point(env_name)
        else:
            self._env_cfg = env_cfg
        # Merge configurations: env_cfg takes precedence over self._env_cfg
        self._env = gym.make(env_name, cfg=self._env_cfg)

    
    def reset(self, seed: Optional[int] = None, options: Optional[dict] = None):
        return self._env.reset(seed=seed, options=options)
    
    def step(self, action):
        return self._env.step(action)
    
    def render(self, mode='human'):
        return self._env.render(mode=mode)
    
    @property
    def unwrapped(self):
        return self._env.unwrapped
    
    def close(self):
        self._env.close()
        # Close the simulation app if it was initialized
        global _simulation_app, _app_launcher
        if _simulation_app is not None:
            try:
                _simulation_app.close()
            except Exception:
                pass  # May already be closed   
            finally:
                _simulation_app = None
                _app_launcher = None
    
    def seed(self, seed: Optional[int] = None):
        return self._env.seed(seed=seed)
    
    def __getattr__(self, name):
        """Delegate attribute access to the wrapped environment."""
        return getattr(self._env, name)




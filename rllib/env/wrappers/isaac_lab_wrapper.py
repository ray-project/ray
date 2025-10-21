import importlib
from typing import Union, Optional
import gymnasium as gym
from gymnasium import spaces
import numpy as np
from isaaclab.envs import DirectRLEnv, ManagerBasedRLEnv
from ray.rllib.utils.annotations import PublicAPI

# Import configuration classes:
from isaaclab.envs import DirectRLEnvCfg, ManagerBasedRLEnvCfg

def _get_cfg_entry_point(env_name, env_cfg_entry_point_key="env_cfg_entry_point"):
    cfg_entry_point = gym.spec(env_name.split(":")[-1]).kwargs.get(env_cfg_entry_point_key)
    if isinstance(cfg_entry_point, str):
        mod_name, attr_name = cfg_entry_point.split(":")
        mod = importlib.import_module(mod_name)
        cfg_cls = getattr(mod, attr_name)
    else:
        cfg_cls = cfg_entry_point
    # load the configuration
    print(f"[INFO]: Parsing configuration from: {cfg_entry_point}")
    if callable(cfg_cls):
        cfg = cfg_cls()
    else:
        cfg = cfg_cls
    return cfg

@PublicAPI
class IsaacLabEnv(gym.Env):
    """A `gym.Env` wrapper for the `Isaac_lab` ."""

    def __init__(self, env_name: str, env_cfg: Optional[Union[DirectRLEnvCfg, ManagerBasedRLEnvCfg]] = None):
        super(IsaacLabEnv, self).__init__()
        self._env_cfg = _get_cfg_entry_point(env_name)
        
        # Merge configurations: env_cfg takes precedence over self._env_cfg
        merged_cfg = self._merge_configs(self._env_cfg, env_cfg)
        self._env = gym.make(env_name, cfg=merged_cfg)

    def _merge_configs(self, base_cfg, user_cfg):
        """
        Merge two configuration objects, giving preference to user_cfg values.
        
        Args:
            base_cfg: Base configuration from the environment entry point
            user_cfg: User-provided configuration (can be incomplete or None)
            
        Returns:
            Merged configuration where user_cfg values override base_cfg values.
            If user_cfg is None, returns base_cfg.
        """
        # If user_cfg is None or empty, return base_cfg
        if user_cfg is None:
            return base_cfg
            
        # Create a copy of the base configuration
        try:
            merged = base_cfg.copy()
        except AttributeError:
            # If copy method doesn't exist, create a new instance of the same class
            merged = type(base_cfg)()
            # Copy all attributes from base_cfg to merged
            for attr_name in dir(base_cfg):
                if not attr_name.startswith('_') and not callable(getattr(base_cfg, attr_name)):
                    try:
                        setattr(merged, attr_name, getattr(base_cfg, attr_name))
                    except (AttributeError, TypeError):
                        pass
            
        # Recursively merge configurations
        for key, value in user_cfg.__dict__.items():
            if not key.startswith('_'):  # Skip private attributes
                if hasattr(merged, key):
                    # If both configs have the same key and the value is a config object, merge recursively
                    if (hasattr(value, '__dict__') and hasattr(getattr(merged, key), '__dict__') 
                        and not isinstance(value, (str, int, float, bool, list, tuple, dict, np.ndarray))):
                        setattr(merged, key, self._merge_configs(getattr(merged, key), value))
                    else:
                        # Otherwise, user_cfg value takes precedence
                        setattr(merged, key, value)
                else:
                    # If base config doesn't have this key, add it from user_cfg
                    setattr(merged, key, value)
        
        return merged


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
        from isaaclab.app import AppLauncher
        _app_launcher = AppLauncher()
        _simulation_app = _app_launcher.app
    return _simulation_app


def _get_cfg_entry_point(env_name, env_cfg_entry_point_key="env_cfg_entry_point"):
    cfg_entry_point = gym.spec(env_name.split(":")[-1]).kwargs.get(env_cfg_entry_point_key)
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
    """A `gym.Env` wrapper for the `Isaac_lab` ."""

    def __init__(self, env_name: str, env_cfg: dict = None):
        super(IsaacLabEnv, self).__init__() 
        # Lazy initialization of simulation app when actually creating the environment
        _get_simulation_app()
        
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



if __name__ == "__main__":
    env = IsaacLabEnv(env_name="Isaac-Ant-v0")
    print(f"[INFO] Environment created successfully!")
    print(f"[INFO] Action space: {env.action_space}")
    print(f"[INFO] Observation space: {env.observation_space}")
    print(f"[INFO] Number of environments: {env.unwrapped.num_envs}")
         
        # Sample a state from the environment
    print("\n[INFO] Sampling state from environment...")
    obs, info = env.reset()
    print(f"[INFO] Sampled observation shape: {obs.shape if hasattr(obs, 'shape') else type(obs)}")
    print(f"[INFO] Sampled observation type: {type(obs)}")
    if hasattr(obs, 'shape') and len(obs.shape) > 1:
        print(f"[INFO] Number of environments in observation: {obs.shape[0]}")
        
        # Sample a random action and step
    action = env.action_space.sample()
    print(f"[INFO] Sampled action shape: {action.shape if hasattr(action, 'shape') else type(action)}")
    env.close()

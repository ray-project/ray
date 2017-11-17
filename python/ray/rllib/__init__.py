from ray.tune.registry import _default_registry, TRAINABLE_CLASS
from ray.rllib.agent import _MockAgent, _SigmoidFakeData

from ray.rllib import ppo, es, dqn, a3c, script_runner


_default_registry.register(TRAINABLE_CLASS, "PPO", ppo.PPOAgent)

_default_registry.register(TRAINABLE_CLASS, "ES", es.ESAgent)

_default_registry.register(TRAINABLE_CLASS, "DQN", dqn.DQNAgent)

_default_registry.register(TRAINABLE_CLASS, "A3C", a3c.A3CAgent)

_default_registry.register(TRAINABLE_CLASS, "script", script_runner.ScriptRunner)

_default_registry.register(TRAINABLE_CLASS, "__fake", _MockAgent)

_default_registry.register(TRAINABLE_CLASS, "__sigmoid_fake_data", _SigmoidFakeData)

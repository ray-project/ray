from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# Note: do not introduce unnecessary library dependencies here, e.g. gym.
# This file is imported from the tune module in order to register RLlib agents.
from ray.tune.registry import register_trainable

from ray.rllib.env import (
    AsyncVectorEnv, MultiAgentEnv, ServingEnv, VectorEnv, EnvContext)
from ray.rllib.evaluation import (
    EvaluatorInterface, CommonPolicyEvaluator, PolicyEvaluator,
    PolicyGraph, TFPolicyGraph,
    TorchPolicyGraph, SampleBatch, MultiAgentBatch, SampleBatchBuilder,
    MultiAgentSampleBatchBuilder, SyncSampler, AsyncSampler,
    compute_advantages, collect_metrics)
from ray.rllib.models import (
    ActionDistribution, Categorical,
    DiagGaussian, Deterministic, ModelCatalog, Model,
    Preprocessor, FullyConnectedNetwork, LSTM)
from ray.rllib.optimizers import (
    PolicyOptimizer, AsyncSamplesOptimizer, AsyncGradientsOptimizer,
    SyncSamplesOptimizer, SyncReplayOptimizer, LocalMultiGPUOptimizer)
from ray.rllib.utils import (
    Filter, FilterManager, PolicyClient, PolicyServer)


def _register_all():
    for key in ["PPO", "ES", "DQN", "APEX", "A3C", "BC", "PG", "DDPG",
                "APEX_DDPG", "__fake", "__sigmoid_fake_data",
                "__parameter_tuning"]:
        from ray.rllib.agents.agent import get_agent_class
        register_trainable(key, get_agent_class(key))


_register_all()

__all__ = [
    # rllib.env
    "AsyncVectorEnv", "MultiAgentEnv", "ServingEnv", "VectorEnv", "EnvContext",

    # rllib.evaluation
    "EvaluatorInterface", "CommonPolicyEvaluator", "PolicyEvaluator",
    "PolicyGraph", "TFPolicyGraph",
    "TorchPolicyGraph", "SampleBatch", "MultiAgentBatch", "SampleBatchBuilder",
    "MultiAgentSampleBatchBuilder", "SyncSampler", "AsyncSampler",
    "compute_advantages", "collect_metrics",

    # rllib.models
    "ActionDistribution", "ActionDistribution", "Categorical",
    "DiagGaussian", "Deterministic", "ModelCatalog", "Model",
    "Preprocessor", "FullyConnectedNetwork", "LSTM",

    # rllib.optimizers
    "PolicyOptimizer", "AsyncSamplesOptimizer", "AsyncGradientsOptimizer",
    "SyncSamplesOptimizer", "SyncReplayOptimizer", "LocalMultiGPUOptimizer",

    # rllib.utils
    "Filter", "FilterManager", "PolicyClient", "PolicyServer"
]

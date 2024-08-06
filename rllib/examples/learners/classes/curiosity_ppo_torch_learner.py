from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.examples.learners.classes.curiosity_torch_learner_utils import (
    make_curiosity_config_class,
    make_curiosity_learner_class,
)


PPOConfigWithCuriosity = make_curiosity_config_class(PPOConfig)
PPOTorchLearnerWithCuriosity = make_curiosity_learner_class(PPOTorchLearner)

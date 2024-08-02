from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.algorithms.dqn.torch.dqn_rainbow_torch_learner import (
    DQNRainbowTorchLearner,
)
from ray.rllib.examples.learners.classes.curiosity_torch_learner_utils import (
    make_curiosity_config_class,
    make_curiosity_learner_class,
)


DQNConfigWithCuriosity = make_curiosity_config_class(DQNConfig)
DQNTorchLearnerWithCuriosity = make_curiosity_learner_class(DQNRainbowTorchLearner)

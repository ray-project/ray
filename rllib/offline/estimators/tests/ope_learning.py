from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel

def test_fqe(behavior, ):
    algo = DQNConfig().environment(env="CartPole-v0").framework("torch")
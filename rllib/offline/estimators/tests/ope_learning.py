from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.policy.sample_batch import concat_samples, SampleBatch
from ray.rllib.utils.numpy import convert_to_numpy
import numpy as np

checkpoint_dir = "/tmp/cartpole/torch/"
num_episodes = 1000

def test_on_batch(eval_checkpoint: str, batch: SampleBatch, target_estimate: float):
    config = (
        DQNConfig()
        .environment(env="CartPole-v0")
        .framework("torch")
        .rollouts(num_rollout_workers=0)
        .exploration(explore=False)
    )
    algo = config.build()
    algo.load_checkpoint(eval_checkpoint)

    fqe = FQETorchModel(
        policy = algo.get_policy(),
        gamma = algo.config["gamma"],
        n_iters = 160,
        minibatch_size=32,
        tau=0.05,
    )
    losses = fqe.train(batch)
    estimates = fqe.estimate_v(batch)
    estimates = convert_to_numpy(estimates)
    print(np.mean(estimates), target_estimate, np.mean(losses))

random_path = checkpoint_dir + "checkpoint/random"
mixed_path = checkpoint_dir + "checkpoint/mixed"
optimal_path = checkpoint_dir + "checkpoint/optimal"

reader = JsonReader(checkpoint_dir + "data/random")
random_batch = concat_samples([reader.next() for _ in range(num_episodes)])

# reader = JsonReader(checkpoint_dir + "data/mixed")
# mixed_batch = concat_samples([reader.next() for _ in range(num_episodes)])

# reader = JsonReader(checkpoint_dir + "data/optimal")
# optimal_batch = concat_samples([reader.next() for _ in range(num_episodes)])

test_on_batch(random_path, random_batch, 0)
# test_on_batch(random_path, random_batch, 120)
# test_on_batch(random_path, random_batch, 200)
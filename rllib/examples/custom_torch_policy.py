import argparse
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch

parser = argparse.ArgumentParser()
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--num-cpus", type=int, default=0)


def policy_gradient_loss(policy, model, dist_class, train_batch):
    logits, _ = model({SampleBatch.CUR_OBS: train_batch[SampleBatch.CUR_OBS]})
    action_dist = dist_class(logits, model)
    log_probs = action_dist.logp(train_batch[SampleBatch.ACTIONS])
    return -train_batch[SampleBatch.REWARDS].dot(log_probs)


# <class 'ray.rllib.policy.torch_policy_template.MyTorchPolicy'>
MyTorchPolicy = build_policy_class(
    name="MyTorchPolicy", framework="torch", loss_fn=policy_gradient_loss
)


# Create a new Algorithm using the Policy defined above.
class MyAlgorithm(Algorithm):
    def get_default_policy_class(self, config):
        return MyTorchPolicy


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    tuner = tune.Tuner(
        MyAlgorithm,
        run_config=air.RunConfig(
            stop={"training_iteration": args.stop_iters},
        ),
        param_space={
            "env": "CartPole-v1",
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "num_workers": 2,
            "framework": "torch",
        },
    )
    tuner.fit()

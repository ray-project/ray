import argparse
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.evaluation.postprocessing import discount_cumsum
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--num-cpus", type=int, default=0)


def policy_gradient_loss(policy, model, dist_class, train_batch):
    logits, _ = model(train_batch)
    action_dist = dist_class(logits, model)
    return -tf.reduce_mean(
        action_dist.logp(train_batch["actions"]) * train_batch["returns"]
    )


def calculate_advantages(policy, sample_batch, other_agent_batches=None, episode=None):
    sample_batch["returns"] = discount_cumsum(sample_batch["rewards"], 0.99)
    return sample_batch


# <class 'ray.rllib.policy.tf_policy_template.MyTFPolicy'>
MyTFPolicy = build_tf_policy(
    name="MyTFPolicy",
    loss_fn=policy_gradient_loss,
    postprocess_fn=calculate_advantages,
)


# Create a new Algorithm using the Policy defined above.
class MyAlgo(Algorithm):
    @classmethod
    def get_default_policy_class(cls, config):
        return MyTFPolicy


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    tuner = tune.Tuner(
        MyAlgo,
        run_config=air.RunConfig(
            stop={"training_iteration": args.stop_iters},
        ),
        param_space={
            "env": "CartPole-v1",
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": float(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "num_workers": 2,
            "framework": "tf",
        },
    )

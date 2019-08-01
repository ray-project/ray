from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray
from ray import tune
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy

parser = argparse.ArgumentParser()
parser.add_argument("--iters", type=int, default=200)


def policy_gradient_loss(policy, batch_tensors):
    logits, _ = policy.model({
        SampleBatch.CUR_OBS: batch_tensors[SampleBatch.CUR_OBS]
    })
    action_dist = policy.dist_class(logits, policy.config["model"])
    log_probs = action_dist.logp(batch_tensors[SampleBatch.ACTIONS])
    return -batch_tensors[SampleBatch.REWARDS].dot(log_probs)


# <class 'ray.rllib.policy.torch_policy_template.MyTorchPolicy'>
MyTorchPolicy = build_torch_policy(
    name="MyTorchPolicy", loss_fn=policy_gradient_loss)

# <class 'ray.rllib.agents.trainer_template.MyCustomTrainer'>
MyTrainer = build_trainer(
    name="MyCustomTrainer",
    default_policy=MyTorchPolicy,
)

if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    tune.run(
        MyTrainer,
        stop={"training_iteration": args.iters},
        config={
            "env": "CartPole-v0",
            "num_workers": 2,
        })

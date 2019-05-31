from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray
from ray import tune
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--iters", type=int, default=200)


def policy_gradient_loss(policy, batch_tensors):
    actions = batch_tensors[SampleBatch.ACTIONS]
    rewards = batch_tensors[SampleBatch.REWARDS]
    return -tf.reduce_mean(policy.action_dist.logp(actions) * rewards)


# <class 'ray.rllib.policy.tf_policy_template.MyTFPolicy'>
MyTFPolicy = build_tf_policy(
    name="MyTFPolicy",
    loss_fn=policy_gradient_loss,
)

# <class 'ray.rllib.agents.trainer_template.MyCustomTrainer'>
MyTrainer = build_trainer(
    name="MyCustomTrainer",
    default_policy=MyTFPolicy,
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

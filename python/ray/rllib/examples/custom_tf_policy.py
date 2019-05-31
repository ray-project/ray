import tensorflow as tf
import ray
from ray import tune
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy


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

ray.init()
tune.run(
    MyTrainer,
    stop={"training_iteration": 1},
    config={
        "env": "CartPole-v0",
        "num_workers": 2,
    })

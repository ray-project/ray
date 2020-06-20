import argparse
import random

import ray
from ray import tune
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.examples.models.eager_model import EagerModel
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=150)
parser.add_argument("--as-test", action="store_true")


def policy_gradient_loss(policy, model, dist_class, train_batch):
    """Example of using embedded eager execution in a custom loss.

    Here `compute_penalty` prints the actions and rewards for debugging, and
    also computes a (dummy) penalty term to add to the loss.
    """

    def compute_penalty(actions, rewards):
        assert tf.executing_eagerly()
        penalty = tf.reduce_mean(tf.cast(actions, tf.float32))
        if random.random() > 0.9:
            print("The eagerly computed penalty is", penalty, actions, rewards)
        return penalty

    logits, _ = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)

    actions = train_batch[SampleBatch.ACTIONS]
    rewards = train_batch[SampleBatch.REWARDS]
    penalty = tf.py_function(
        compute_penalty, [actions, rewards], Tout=tf.float32)

    return penalty - tf.reduce_mean(action_dist.logp(actions) * rewards)


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
    ModelCatalog.register_custom_model("eager_model", EagerModel)

    config = {
        "env": "CartPole-v0",
        "num_workers": 0,
        "model": {
            "custom_model": "eager_model"
        },
        "framework": "tfe",
    }
    stop = {
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(MyTrainer, stop=stop, config=config)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()

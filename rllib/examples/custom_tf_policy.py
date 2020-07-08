import argparse

import ray
from ray import tune
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.evaluation.postprocessing import discount
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--num-cpus", type=int, default=0)


def policy_gradient_loss(policy, model, dist_class, train_batch):
    logits, _ = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)
    return -tf.reduce_mean(
        action_dist.logp(train_batch["actions"]) * train_batch["returns"])


def calculate_advantages(policy,
                         sample_batch,
                         other_agent_batches=None,
                         episode=None):
    sample_batch["returns"] = discount(sample_batch["rewards"], 0.99)
    return sample_batch


# <class 'ray.rllib.policy.tf_policy_template.MyTFPolicy'>
MyTFPolicy = build_tf_policy(
    name="MyTFPolicy",
    loss_fn=policy_gradient_loss,
    postprocess_fn=calculate_advantages,
)

# <class 'ray.rllib.agents.trainer_template.MyCustomTrainer'>
MyTrainer = build_trainer(
    name="MyCustomTrainer",
    default_policy=MyTFPolicy,
)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    tune.run(
        MyTrainer,
        stop={"training_iteration": args.stop_iters},
        config={
            "env": "CartPole-v0",
            "num_workers": 2,
            "framework": "tf",
        })

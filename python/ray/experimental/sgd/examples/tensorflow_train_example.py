from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from ray import tune
from ray.experimental.sgd.tensorflow.tensorflow_trainer import (TensorFlowTrainer,
                                                          TensorFlowTrainable)

from ray.experimental.sgd.tests.tf_helper import (
    get_model, get_dataset)

def train_example(num_replicas=1, use_gpu=False):
    trainer = TensorFlowTrainer(
        model_creator=get_model,
        data_creator=get_dataset,
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        batch_size=512)

    train_stats1 = trainer.train()
    train_stats1.update(trainer.validate())
    print(train_stats1)

    train_stats2 = trainer.train()
    train_stats2.update(trainer.validate())
    print(train_stats2)

    assert train_stats1["train_loss"] > train_stats2["train_loss"]

    val_stats = trainer.validate()
    print(val_stats)
    print("success!")


def tune_example(num_replicas=1, use_gpu=False):
    config = {
        "model_creator": tune.function(get_model),
        "data_creator": tune.function(get_dataset),
        "num_replicas": num_replicas,
        "use_gpu": use_gpu,
        "batch_size": 512
    }

    analysis = tune.run(
        TensorFlowTrainable,
        num_samples=12,
        config=config,
        stop={"training_iteration": 2},
        verbose=1)

    return analysis.get_best_config(metric="validation_loss", mode="min")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-address",
        required=False,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Sets number of replicas for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")

    args, _ = parser.parse_known_args()

    import ray

    ray.init(redis_address=args.redis_address)

    if args.tune:
        tune_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)
    else:
        train_example(num_replicas=args.num_replicas, use_gpu=args.use_gpu)

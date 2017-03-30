from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray
import argparse

import objective

parser = argparse.ArgumentParser(description="Run the hyperparameter optimization example.")
parser.add_argument("--trials", default=2, type=int, help="The number of random trials to do.")
parser.add_argument("--steps", default=10, type=int, help="The number of steps of training to do per network.")
parser.add_argument("--redis-address", default=None, type=str, help="The Redis address of the cluster.")

if __name__ == "__main__":
  args = parser.parse_args()

  ray.init(redis_address=args.redis_address)

  # The number of sets of random hyperparameters to try.
  trials = args.trials
  # The number of training passes over the dataset to use for network.
  steps = args.steps

  # Keep track of the best hyperparameters and the best accuracy.
  best_hyperparamemeters = None
  best_accuracy = 0
  # This list holds the object IDs for all of the experiments that we have
  # launched and that have not yet been processed.
  remaining_ids = []
  # This is a dictionary mapping the object ID of an experiment to the
  # hyerparameters used for that experiment.
  hyperparameters_mapping = {}

  # A function for generating random hyperparameters.
  def generate_hyperparameters():
    return {"learning_rate": 10 ** np.random.uniform(-3, -1),
            "batch_size": np.random.randint(1, 100)}

  # Randomly generate some hyperparameters, and launch a task for each set.
  for i in range(trials):
    hyperparameters = generate_hyperparameters()
    accuracy_id = objective.train_cnn_and_compute_accuracy.remote(
        hyperparameters, steps)
    remaining_ids.append(accuracy_id)
    # Keep track of which hyperparameters correspond to this experiment.
    hyperparameters_mapping[accuracy_id] = hyperparameters

  # Fetch and print the results of the tasks in the order that they complete.
  for i in range(trials):
    # Use ray.wait to get the object ID of the first task that completes.
    ready_ids, remaining_ids = ray.wait(remaining_ids)
    # Process the output of this task.
    result_id = ready_ids[0]
    hyperparameters = hyperparameters_mapping[result_id]
    accuracy, _ = ray.get(result_id)
    print("""We achieve accuracy {:.3}% with
        learning_rate: {:.2}
        batch_size: {}
      """.format(100 * accuracy,
                 hyperparameters["learning_rate"],
                 hyperparameters["batch_size"]))
    if accuracy > best_accuracy:
      best_hyperparameters = hyperparameters
      best_accuracy = accuracy

  # Record the best performing set of hyperparameters.
  print("""Best accuracy over {} trials was {:.3} with
        learning_rate: {:.2}
        batch_size: {}
    """.format(trials, 100 * best_accuracy,
               best_hyperparameters["learning_rate"],
               best_hyperparameters["batch_size"]))

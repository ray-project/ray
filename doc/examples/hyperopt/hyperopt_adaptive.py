from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from collections import defaultdict
import numpy as np
import ray

from tensorflow.examples.tutorials.mnist import input_data

import objective

parser = argparse.ArgumentParser(description="Run the hyperparameter "
                                             "optimization example.")
parser.add_argument("--num-starting-segments", default=5, type=int,
                    help="The number of training segments to start in "
                         "parallel.")
parser.add_argument("--num-segments", default=10, type=int,
                    help="The number of additional training segments to "
                         "perform.")
parser.add_argument("--steps-per-segment", default=20, type=int,
                    help="The number of steps of training to do per training "
                         "segment.")
parser.add_argument("--redis-address", default=None, type=str,
                    help="The Redis address of the cluster.")


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(redis_address=args.redis_address)

    # The number of training passes over the dataset to use for network.
    steps = args.steps_per_segment

    # Load the mnist data and turn the data into remote objects.
    print("Downloading the MNIST dataset. This may take a minute.")
    mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
    train_images = ray.put(mnist.train.images)
    train_labels = ray.put(mnist.train.labels)
    validation_images = ray.put(mnist.validation.images)
    validation_labels = ray.put(mnist.validation.labels)

    # Keep track of the accuracies that we've seen at different numbers of
    # iterations.
    accuracies_by_num_steps = defaultdict(lambda: [])

    # Define a method to determine if an experiment looks promising or not.
    def is_promising(experiment_info):
        accuracies = experiment_info["accuracies"]
        total_num_steps = experiment_info["total_num_steps"]
        comparable_accuracies = accuracies_by_num_steps[total_num_steps]
        if len(comparable_accuracies) == 0:
            if len(accuracies) == 1:
                # This means that we haven't seen anything finish yet, so keep
                # running this experiment.
                return True
            else:
                # The experiment is promising if the second half of the
                # accuracies are better than the first half of the accuracies.
                return (np.mean(accuracies[:len(accuracies) // 2]) <
                        np.mean(accuracies[len(accuracies) // 2:]))
        # Otherwise, continue running the experiment if it is in the top half
        # of experiments we've seen so far at this point in time.
        return np.mean(accuracy > np.array(comparable_accuracies)) > 0.5

    # Keep track of all of the experiment segments that we're running. This
    # dictionary uses the object ID of the experiment as the key.
    experiment_info = {}
    # Keep track of the curently running experiment IDs.
    remaining_ids = []

    # Keep track of the best hyperparameters and the best accuracy.
    best_hyperparameters = None
    best_accuracy = 0

    # A function for generating random hyperparameters.
    def generate_hyperparameters():
        return {"learning_rate": 10 ** np.random.uniform(-5, 5),
                "batch_size": np.random.randint(1, 100),
                "dropout": np.random.uniform(0, 1),
                "stddev": 10 ** np.random.uniform(-5, 5)}

    # Launch some initial experiments.
    for _ in range(args.num_starting_segments):
        hyperparameters = generate_hyperparameters()
        experiment_id = objective.train_cnn_and_compute_accuracy.remote(
            hyperparameters, steps, train_images, train_labels,
            validation_images, validation_labels)
        experiment_info[experiment_id] = {"hyperparameters": hyperparameters,
                                          "total_num_steps": steps,
                                          "accuracies": []}
        remaining_ids.append(experiment_id)

    for _ in range(args.num_segments):
        # Wait for a segment of an experiment to finish.
        ready_ids, remaining_ids = ray.wait(remaining_ids, num_returns=1)
        experiment_id = ready_ids[0]
        # Get the accuracy and the weights.
        accuracy, weights = ray.get(experiment_id)
        # Update the experiment info.
        previous_info = experiment_info[experiment_id]
        previous_info["accuracies"].append(accuracy)

        # Update the best accuracy and best hyperparameters.
        if accuracy > best_accuracy:
            best_hyperparameters = previous_info["hyperparameters"]
            best_accuracy = accuracy

        if is_promising(previous_info):
            # If the experiment still looks promising, then continue running
            # it.
            print("Continuing to run the experiment with hyperparameters {}."
                  .format(previous_info["hyperparameters"]))
            new_hyperparameters = previous_info["hyperparameters"]
            new_info = {"hyperparameters": new_hyperparameters,
                        "total_num_steps": (previous_info["total_num_steps"] +
                                            steps),
                        "accuracies": previous_info["accuracies"][:]}
            starting_weights = weights
        else:
            # If the experiment does not look promising, start a new
            # experiment.
            print("Ending the experiment with hyperparameters {}."
                  .format(previous_info["hyperparameters"]))
            new_hyperparameters = generate_hyperparameters()
            new_info = {"hyperparameters": new_hyperparameters,
                        "total_num_steps": steps,
                        "accuracies": []}
            starting_weights = None

        # Start running the next segment.
        new_experiment_id = objective.train_cnn_and_compute_accuracy.remote(
            new_hyperparameters, steps, train_images, train_labels,
            validation_images, validation_labels, weights=starting_weights)
        experiment_info[new_experiment_id] = new_info
        remaining_ids.append(new_experiment_id)

        # Update the set of all accuracies that we've seen.
        accuracies_by_num_steps[previous_info["total_num_steps"]].append(
            accuracy)

    # Record the best performing set of hyperparameters.
    print("""Best accuracy was {:.3} with
          learning_rate: {:.2}
          batch_size: {}
          dropout: {:.2}
          stddev: {:.2}
      """.format(100 * best_accuracy,
                 best_hyperparameters["learning_rate"],
                 best_hyperparameters["batch_size"],
                 best_hyperparameters["dropout"],
                 best_hyperparameters["stddev"]))

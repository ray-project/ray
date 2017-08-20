#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import gym

import ray
from ray.rllib.a3c import A2C, A3C, DEFAULT_CONFIG, shared_model, shared_model_lstm


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the A3C algorithm.")
    parser.add_argument("--environment", default="PongDeterministic-v4",
                        type=str, help="The gym environment to use.")
    parser.add_argument("--redis-address", default=None, type=str,
                        help="The Redis address of the cluster.")
    parser.add_argument("--num-workers", default=4, type=int,
                        help="The number of A3C workers to use.")
    parser.add_argument("--iterations", default=-1, type=int,
                        help="The number of training iterations to run.")
    parser.add_argument("--k-step", default=None, type=int,
                        help="Minibatch size for each worker gradient")
    parser.add_argument("--num-batches", default=20, type=int,
                        help="Number of batches per iteration")

    args = parser.parse_args()
    ray.init(redis_address=args.redis_address, num_cpus=args.num_workers)

    config = DEFAULT_CONFIG.copy()
    config["num_workers"] = args.num_workers
    if args.k_step:
        config["batch_size"] = args.k_step
    # assert args.num_batches is None, "A2C doesn't have a batch count"
    if args.num_batches:
        config["num_batches_per_iteration"] = args.num_batches
    #policy_class = LSTM.LSTMPolicy
    if args.environment[:4] == "Pong":
        policy_class = shared_model_lstm.SharedModelLSTM
    # else:

    a2c = A2C(args.environment, policy_class, config)

    iteration = 0
    import time
    start = time.time()
    times = []
    goals = [500, 1000, 2000]
    while iteration != args.iterations:
        iteration += 1
        res = a2c.train()
        print("current status: {}".format(res))
        if res.info['last_batch'] >= goals[len(times)]:
            times.append((iteration, time.time() - start))
            if len(times) == len(goals):
                break
    v = " ".join(["%d_Iter:%d %d_Sec:%0.4f" % (g, i, g, s) for g, (i, s) in zip(goals, times)]) 
    print(v)
    with open("a2c_results.txt", "a") as f:
        f.write("For " + " ".join([str(k) + ":" + str(v) for k, v in sorted(config.items())]) + "\n")
        f.write(v + "\n") 
        f.write("\n")

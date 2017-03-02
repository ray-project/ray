import ray
import numpy as np
from runner import RunnerThread, process_rollout
from LSTM import LSTMPolicy, RawLSTMPolicy
import tensorflow as tf
import six.moves.queue as queue
import gym
from datetime import datetime, timedelta
from misc import timestamp, Profiler
from envs import create_env
import logging
from collections import defaultdict

#debug

import cProfile, pstats, io


@ray.actor
class Runner(object):
    def __init__(self, env_name, id, start=True):
        env = create_env(env_name, None, None)
        num_actions = env.action_space.n
        self.policy = LSTMPolicy(env.observation_space.shape, num_actions, id)
        self.runner = RunnerThread(env, self.policy, 20)
        self.id = id
        self.env = env
        self.logdir = "tmp/"
        self.prev_fin = timestamp()
        if start:
            self.start()

    def pull_batch_from_queue(self):
        """ self explanatory:  take a rollout from the queue of the thread runner. """
        rollout = self.runner.queue.get(timeout=600.0)
        while not rollout.terminal:
            try:
                rollout.extend(self.runner.queue.get_nowait())
            except queue.Empty:
                break
        return rollout

    def start(self):
        summary_writer = tf.train.SummaryWriter(self.logdir + "test_1")
        self.summary_writer = summary_writer
        self.runner.start_runner(self.policy.sess, summary_writer)

    def compute_gradient(self, params, debug=None):
        self.policy.set_weights(params)
        _start = timestamp()
        rollout = self.pull_batch_from_queue()
        batch = process_rollout(rollout, gamma=0.99, lambda_=1.0)
        _s = timestamp() 
        gradient = self.policy.get_gradients(batch, profile=False)
        info = {"id": self.id, "prev": self.prev_fin, "end": timestamp(), "size": len(batch.a), "launch": debug['time'], "start":_start, "pull": _s}
        self.prev_fin = info["end"]
        return gradient, info


def train(num_workers, env_name="PongDeterministic-v3",
                         debug=False,
                         policy_cls=None):
    env = create_env(env_name, None, None)
    if policy_cls is not None:
        policy = policy_cls(env.observation_space.shape, env.action_space.n, 0)
    else:
        policy = LSTMPolicy(env.observation_space.shape, env.action_space.n, 0)
    agents = [Runner(env_name, i) for i in range(num_workers)]
    parameters = policy.get_weights()
    gradients = [agent.compute_gradient(parameters, {"time": timestamp()}) for agent in agents]
    steps = 0
    obs = 0
    START = timestamp()
    log = defaultdict(int)
    now = timestamp()
    while True:
        done_id, gradients = ray.wait(gradients)
        cll_time = timestamp()
        gradient, info = ray.get(done_id)[0]
        rayget_time = timestamp()
        policy.model_update(gradient)
        update_time = timestamp()
        parameters = policy.get_weights()
        steps += 1
        obs += info["size"]
        getw_time = timestamp()
        gradients.extend([agents[info["id"]].compute_gradient(parameters, {"time": timestamp()})])
        prev_now = now 
        now = timestamp()
        if debug:
            log["1. Wait"] += ((cll_time - prev_now) )
            log["2. Ray Get"] += ((rayget_time - cll_time))
            log["3. Apply"] += ((update_time - rayget_time) )
            log["4. Get tf weights"] += ((getw_time - update_time) )
            # log["Idle (w)"] += ((info['launch'] - info['prev']))
            # log["Ship (w)"] += ((info['start'] - info['launch']))
            # log["Pull (w)"] += ((info['pull'] - info['start']))
            # log["Grad Calc (w)"] += ((info['end'] - info['pull']))
            # log["Sit (w)"] += ((cll_time - info['end']))
            log["5. New"] += ((now - getw_time) )

            if steps % 50 == 0:
                print("[%d: %s] %d steps.. (%0.4f obs/sec, %0.4f sec/grad)" % (steps, 
                                                                    str(timedelta(seconds=now - START)), 
                                                                    obs, 
                                                                    obs / (now - START),
                                                                    (now - START) / steps))
                log_str = []
                for label, val in sorted(log.items()):
                    log_str.append("%s: %.4fs" % (label, val / steps))
                print(" ".join(log_str))
    return policy

if __name__ == '__main__':
    import sys
    NW = int(sys.argv[1])
    debug = False
    if len(sys.argv) == 3 and sys.argv[2] == "debug":
        debug = True
    ray.init(num_workers=NW, num_cpus=NW)
    train(NW, debug=debug) #, policy_cls=RawLSTMPolicy)#, env_name="CartPole-v0")

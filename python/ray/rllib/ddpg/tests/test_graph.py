import tensorflow as tf
import gym
from ddpg import DEFAULT_CONFIG
from collections import deque
from models import DDPGGraph
import numpy as np
import random

from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.optimizers.replay_buffer import ReplayBuffer, \
    PrioritizedReplayBuffer
from ray.rllib.utils.compression import pack
from ray.rllib.ddpg.common.schedules import ConstantSchedule, LinearSchedule
from ray.rllib.ddpg.common.wrappers import wrap_ddpg

def main():
    #env = gym.make("Pendulum-v0")
    env_creator = lambda env_config: gym.make("Pendulum-v0")
    env = env_creator("Pendulum-v0")
    env = wrap_ddpg(dict(), env, DEFAULT_CONFIG["model"], False)
    sess = tf.Session()
    ddpg = DDPGGraph(dict(), env, DEFAULT_CONFIG, "./temp")
    sess.run(tf.global_variables_initializer())
    ddpg.update_target_hard(sess)

    #memory = deque(maxlen=DEFAULT_CONFIG["buffer_size"])
    #memory = ReplayBuffer(DEFAULT_CONFIG["buffer_size"], False)
    memory = PrioritizedReplayBuffer(DEFAULT_CONFIG["buffer_size"], 0.6, False)

    eps = 0.1
    exploration = LinearSchedule(
                schedule_timesteps=int(
                    0.4 *
                    DEFAULT_CONFIG["schedule_max_timesteps"]),
                initial_p=DEFAULT_CONFIG["noise_scale"] * 1.0,
                final_p=DEFAULT_CONFIG["noise_scale"] * DEFAULT_CONFIG["exploration_final_eps"])
    num_ts = 0
    ep_rwds = list()
    last_sync_target = 0
    
    for t in xrange(1000):
        ob = env.reset()
        done = False
        ep_rwd = .0
        #eps = 0.1 * 0.99 ** t
        eps = exploration.value(num_ts)

        while not done:
            act = ddpg.act(sess, np.array(ob)[None], eps)[0]

            new_ob, rew, done, _ = env.step(act)
            num_ts += 1
            #memory.append((ob, act, rew, new_ob, float(done)))
            obs, actions, rewards, new_obs, dones = [], [], [], [], []
            obs.append(ob)
            actions.append(act)
            rewards.append(rew)
            new_obs.append(new_ob)
            dones.append(float(done))
            batch = SampleBatch({
            "obs": [pack(np.array(o)) for o in obs], "actions": actions,
            "rewards": rewards,
            "new_obs": [pack(np.array(o)) for o in new_obs], "dones": dones,
            "weights": np.ones_like(rewards)})
            for row in batch.rows():
                memory.add(
                    row["obs"], row["actions"], row["rewards"], row["new_obs"],
                    row["dones"], row["weights"])



            if num_ts % DEFAULT_CONFIG["sample_batch_size"] == 0 and len(memory) >= DEFAULT_CONFIG["learning_starts"]:
                '''
                batch = random.sample(memory, DEFAULT_CONFIG["train_batch_size"])
                obses_t = np.asarray([elem[0] for elem in batch])
                actions = np.asarray([elem[1] for elem in batch])
                rewards = np.asarray([elem[2] for elem in batch])
                obses_tp1 = np.asarray([elem[3] for elem in batch])
                dones = np.asarray([elem[4] for elem in batch])
                weights = np.ones_like(rewards)
                batch_indexes = - np.ones_like(rewards)
                samples = SampleBatch({
                    "obs": obses_t, "actions": actions, "rewards": rewards,
                    "new_obs": obses_tp1, "dones": dones, "weights": weights,
                    "batch_indexes": batch_indexes})
                '''
                '''
                (obses_t, actions, rewards, obses_tp1,
                    dones) = memory.sample(
                        DEFAULT_CONFIG["train_batch_size"])
                weights = np.ones_like(rewards)
                batch_indexes = - np.ones_like(rewards)
                samples = SampleBatch({
                    "obs": obses_t, "actions": actions, "rewards": rewards,
                    "new_obs": obses_tp1, "dones": dones, "weights": weights,
                    "batch_indexes": batch_indexes})
                '''
                (obses_t, actions, rewards, obses_tp1,
                    dones, weights, batch_indexes) = memory.sample(
                        DEFAULT_CONFIG["train_batch_size"], 0.4)
                samples = SampleBatch({
                    "obs": obses_t, "actions": actions, "rewards": rewards,
                    "new_obs": obses_tp1, "dones": dones, "weights": weights,
                    "batch_indexes": batch_indexes})
                '''
                print(samples["obs"])
                print(samples["actions"])
                print(samples["rewards"])
                print(samples["new_obs"])
                print(samples["dones"])
                print(samples["weights"])
                raw_input()
                '''
                td_error = ddpg.compute_apply(
                    sess, samples["obs"], samples["actions"], samples["rewards"],
                    samples["new_obs"], samples["dones"], samples["weights"])
                new_priorities = (
                    np.abs(td_error) + 1e-6)
                if isinstance(memory, PrioritizedReplayBuffer):
                    memory.update_priorities(
                        samples["batch_indexes"], new_priorities)
                #if num_ts - last_sync_target >= 500:
                #    ddpg.update_target_hard(sess)
                #    last_sync_target = num_ts
                ddpg.update_target(sess)

            ep_rwd += rew
            if done:
                ep_rwds.append(ep_rwd)
                mean_rwd = np.mean(ep_rwds[-min(t, 100):])
                print("%d %.3f %.3f %.3f" % (t, ep_rwd, eps, mean_rwd))
                ddpg.reset_noise(sess)
                break
            ob = new_ob

if __name__=="__main__":
    main()


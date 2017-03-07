import ray

from reinforce.agent import Agent, RemoteAgent
from reinforce.rollout import collect_samples
from reinforce.utils import iterate, shuffle

config = {"kl_coeff": 0.2,
          "num_sgd_iter": 30,
          "sgd_stepsize": 5e-5,
          "sgd_batchsize": 128,
          "entropy_coeff": 0.0,
          "clip_param": 0.3,
          "kl_target": 0.01,
          "timesteps_per_batch": 40000}

ray.init()

# mdp_name = "Walker2d-v1"
mdp_name = "Pong-ramDeterministic-v3"

agents = [RemoteAgent(mdp_name, 1, config, False) for _ in range(5)]
agent = Agent(mdp_name, 1, config, True)

kl_coeff = config["kl_coeff"]

for j in range(1000):
  print("== iteration", j)
  weights = agent.get_weights()
  [a.load_weights(weights) for a in agents]
  trajectory, total_reward, traj_len_mean = collect_samples(agents, config["timesteps_per_batch"], 0.995, 1.0, 2000)
  print("total reward is ", total_reward)
  print("trajectory length mean is ", traj_len_mean)
  print("timesteps: ", trajectory["dones"].shape[0])
  trajectory["advantages"] = (trajectory["advantages"] - trajectory["advantages"].mean()) / trajectory["advantages"].std()
  print("Computing policy (optimizer='" + agent.optimizer.get_name() + "', iterations=" + str(config["num_sgd_iter"]) + ", stepsize=" + str(config["sgd_stepsize"]) + "):")
  names = ["iter", "loss", "kl", "entropy"]
  print(("{:>15}" * len(names)).format(*names))
  trajectory = shuffle(trajectory)
  ppo = agent.ppo
  for i in range(config["num_sgd_iter"]):
    # Test on current set of rollouts
    loss, kl, entropy = agent.sess.run([ppo.loss, ppo.mean_kl, ppo.mean_entropy],
                            feed_dict={ppo.observations: trajectory["observations"],
                                       ppo.advantages: trajectory["advantages"],
                                       ppo.actions: trajectory["actions"].squeeze(),
                                       ppo.prev_logits: trajectory["logprobs"],
                                       ppo.kl_coeff: kl_coeff})
    print("{:>15}{:15.5e}{:15.5e}{:15.5e}".format(i, loss, kl, entropy))
    # Run SGD for training on current set of rollouts
    for batch in iterate(trajectory, config["sgd_batchsize"]):
      agent.sess.run([agent.train_op],
                                    feed_dict={ppo.observations: batch["observations"],
                                               ppo.advantages: batch["advantages"],
                                               ppo.actions: batch["actions"].squeeze(),
                                               ppo.prev_logits: batch["logprobs"],
                                               ppo.kl_coeff: kl_coeff})
  if kl > 2.0 * config["kl_target"]:
    kl_coeff *= 1.5
  elif kl < 0.5 * config["kl_target"]:
    kl_coeff *= 0.5
  print("kl div = ", kl)
  print("kl coeff = ", kl_coeff)

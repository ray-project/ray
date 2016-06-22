import matplotlib
from matplotlib import pyplot as plt
import pickle
import numpy as np

matplotlib.use("Agg")

logs = pickle.load(open("logs_rl_original.p", "rb"))
times_og = range(1, (len(logs) + 1))
reward_og = map(lambda x:x[2], logs)
plt.plot(times_og, reward_og)
plt.savefig("original_batchnum_graph")
logs = pickle.load(open("logs_rl_ray.p", "rb"))
times_ray = range(1, (len(logs) + 1))
reward_ray = map(lambda x: x[2], logs)
plt.plot(times_ray, reward_ray)
plt.savefig("rl_pong_graph")

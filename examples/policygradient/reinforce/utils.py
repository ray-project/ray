import numpy as np

def flatten(weights, start=0, stop=2):
  for key, val in weights.items():
    dims = val.shape[0:start] + (-1,) + val.shape[stop:]
    # XXX
    weights[key] = val.reshape(dims)[1:]
  return weights

def concatenate(weights_list):
  keys = weights_list[0].keys()
  result = {}
  for key in keys:
    result[key] = np.concatenate([l[key] for l in weights_list])
  return result

def shuffle(trajectory):
  permutation = np.random.permutation(trajectory["dones"].shape[0])
  for key, val in trajectory.items():
    trajectory[key] = val[permutation][permutation]
  return trajectory

def iterate(trajectory, batchsize):
  trajectory = shuffle(trajectory)
  curr_index = 0
  # XXX consume the whole batch
  while curr_index + batchsize < trajectory["dones"].shape[0]:
    batch = dict()
    for key in trajectory:
      batch[key] = trajectory[key][curr_index:curr_index+batchsize]
    curr_index += batchsize
    yield batch

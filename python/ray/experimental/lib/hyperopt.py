from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
import random

import ray

def sample_uniformly(configs):
  """Sample a hyperparameter configuration uniformly at random.
  
  This method takes an OrderedDict, mapping a string to a list of possible
  values.
  """
  return OrderedDict([(key, random.choice(val)) for key, val in configs.items()])

class HyperoptRun(object):

  def __init__(self, algorithm, runs):
    self.algorithm = algorithm
    self.runs = runs

  def num_finished(self):
    ready, waiting = ray.wait(self.runs, timeout=0)
    return len(ready)

  def best_so_far(self):
    # TODO(pcm): Implement this!
    pass

def exhaustive(function, args):
  return HyperoptRun("exhaustive", [function.remote(*arg) for arg in args])

def adaptive(function, args, num_workers):
  arg_iter = iter(args)
  result_dict = dict()
  active_tasks = []
  # Launch set of initial tasks
  for worker_index in range(num_workers):
    arg = next(arg_iter)
    result_id = function.remote(*arg)
    active_tasks.append(result_id)
    result_dict[result_id] = arg
  # Wait for tasks being finished and launch new ones
  for arg in arg_iter:
    [finished_id], _ = ray.wait(active_tasks)
    worker_index = active_tasks.index(finished_id)
    result_id = function.remote(*arg)
    active_tasks[worker_index] = result_id
    result_dict[result_id] = arg
  return result_dict

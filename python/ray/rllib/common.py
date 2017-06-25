from collections import namedtuple


TrainingResult = namedtuple("TrainingResult", [
    "training_iteration",
    "episode_reward_mean",
    "episode_len_mean",
])


class Algorithm(object):
  """All RLlib algorithms extend this base class.

  Algorithm objects retain internal model state between calls to train(), so
  you should create a new algorithm instance for each training session.

  TODO(ekl): support checkpoint / restore of training state.
  """

  def __init__(self, env_name, config):
    self.env_name = env_name
    self.config = config

  def train(self):
    """
    Runs one logical iteration of training.

    Returns:
      A TrainingResult that describes training progress.
    """

    raise NotImplementedError

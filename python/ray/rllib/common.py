from collections import namedtuple
from datetime import datetime
import json
import logging
import tempfile
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


TrainingResult = namedtuple("TrainingResult", [
    "training_iteration",
    "episode_reward_mean",
    "episode_len_mean",
])


class Algorithm(object):
  """All RLlib algorithms extend this base class.

  Algorithm objects retain internal model state between calls to train(), so
  you should create a new algorithm instance for each training session.

  Attributes:
      env_name (str): Name of the OpenAI gym environment to train against.
      config (obj): Algorithm-specific configuration data.
      logdir (str): Directory in which training outputs should be placed.

  TODO(ekl): support checkpoint / restore of training state.
  """

  def __init__(self, env_name, config):
    self.env_name = env_name
    self.config = config
    self.logdir = tempfile.mkdtemp(
        prefix="{}_{}_{}".format(
            env_name,
            self.__class__.__name__,
            datetime.today().strftime("%Y-%m-%d_%H-%M-%S")),
        dir='/tmp/ray')
    json.dump(
        self.config, open(os.path.join(self.logdir, 'config.json'), 'w'),
        sort_keys=True, indent=4)
    logger.info(
        "%s algorithm created with logdir '%s'",
        self.__class__.__name__, self.logdir)

  def train(self):
    """Runs one logical iteration of training.

    Returns:
      A TrainingResult that describes training progress.
    """

    raise NotImplementedError

from collections import namedtuple
from datetime import datetime
import json
import logging
import os
try:
    import smart_open
except ImportError:
    pass
import tempfile

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

  def __init__(self, env_name, config, s3_bucket=None):
    self.env_name = env_name
    self.config = config
    self.logprefix = "{}_{}_{}".format(
            env_name,
            self.__class__.__name__,
            datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))
    self.logdir = tempfile.mkdtemp(prefix=self.logprefix, dir="/tmp/ray")
    if s3_bucket:
      with smart_open.smart_open(s3_bucket + "/" + self.logprefix + "/" + "config.json", "wb") as f:
        json.dump(self.config, f, sort_keys=True, indent=4)
    json.dump(
        self.config, open(os.path.join(self.logdir, "config.json"), "w"),
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

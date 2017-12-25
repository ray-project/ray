from ray.rllib.models import ModelCatalog
from ray.rllib.models.preprocessors import Preprocessor


class FakePreprocessor(Preprocessor):
    def _init(self):
        pass


class FakeEnv(object):
    def __init__(self):
        self.observation_space = lambda: None
        self.observation_space.shape = ()
        self.spec = lambda: None
        self.spec.id = "FakeEnv-v0"


def test_preprocessor():
    ModelCatalog.register_preprocessor("FakeEnv-v0", FakePreprocessor)
    env = FakeEnv()
    preprocessor = ModelCatalog.get_preprocessor(env)
    assert type(preprocessor) == FakePreprocessor

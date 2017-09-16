from ray.rllib.models import ModelCatalog
from ray.rllib.models.preprocessors import Preprocessor


class FakePreprocessor(Preprocessor):

    def __init__(self, options):
        pass


def test_preprocessor():
    ModelCatalog.register_preprocessor("FakeEnv-v0", FakePreprocessor)
    preprocessor = ModelCatalog.get_preprocessor("FakeEnv-v0", (1, 1))
    assert type(preprocessor) == FakePreprocessor

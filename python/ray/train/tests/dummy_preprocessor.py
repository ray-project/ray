from ray.data.preprocessor import Preprocessor

class DummyPreprocessor(Preprocessor):
    def __init__(self, id=None):
        self.id = id

    def transform_batch(self, df):
        self._batch_transformed = True
        return df

    def __eq__(self, other_preprocessor):
        return self.id == other_preprocessor.id


def assert_preprocessor_used(preprocessor: DummyPreprocessor):
    assert hasattr(preprocessor, "_batch_transformed"), (
        "Must use DummyPreprocessor and assert after calling `predict`"
    )

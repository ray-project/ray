import uuid

from ray.data.preprocessor import Preprocessor


class DummyPreprocessor(Preprocessor):
    _is_fittable = False

    def __init__(self, transform=None):
        self.id = uuid.uuid4()

        if transform is None:
            self.transform = lambda b: b
        else:
            self.transform = transform

    def transform_batch(self, batch):
        self._batch_transformed = True
        return self.transform(batch)

    def _transform_pandas(self, df):
        return df

    @property
    def has_preprocessed(self):
        return hasattr(self, "_batch_transformed")

    def __eq__(self, other_preprocessor):
        return self.id == other_preprocessor.id

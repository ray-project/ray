# __statsmodelpredictor_impl_start__
import os
from typing import Optional

import pandas as pd
from statsmodels.base.model import Results
from statsmodels.regression.linear_model import OLSResults

import ray
from ray.air import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train.predictor import Predictor


class StatsmodelPredictor(Predictor):

    def __init__(self, results: Results, preprocessor: Optional[Preprocessor] = None):
        self.results = results
        super().__init__(preprocessor)

    def _predict_pandas(self, data: pd.DataFrame) -> pd.DataFrame:
        predictions: pd.Series = self.results.predict(data)
        return predictions.to_frame(name="predictions")

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        filename: str,
        preprocessor: Optional[Preprocessor] = None,
    ) -> Predictor:
        with checkpoint.as_directory() as directory:
            path = os.path.join(directory, filename)
            results = OLSResults.load(path)
        return cls(results, preprocessor)
# __statsmodelpredictor_impl_end__

# __statsmodelpredictor_usage_start__
import statsmodels.api as sm
import statsmodels.formula.api as smf

from ray.train.batch_predictor import BatchPredictor

dataset: pd.DataFrame = sm.datasets.get_rdataset("Guerry", "HistData").data
results = smf.ols('Lottery ~ Literacy + np.log(Pop1831)', data=dataset).fit()

os.makedirs("checkpoint", exist_ok=True)
results.save("checkpoint/guerry.pickle")
checkpoint = Checkpoint.from_directory("checkpoint")

predictor = BatchPredictor.from_checkpoint(checkpoint, StatsmodelPredictor, filename="guerry.pickle")
# __statsmodelpredictor_usage_end__

# NOTE: This is to ensure the code runs. It shouldn't be part of the documentation.
dataset = ray.data.from_pandas(dataset)
predictor.predict(dataset)

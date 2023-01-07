# fmt: off
# __statsmodelpredictor_imports_start__
import os
from typing import Optional

import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.base.model import Results
from statsmodels.regression.linear_model import OLSResults

import ray
from ray.air import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import Predictor
# __statsmodelpredictor_imports_end__


# __statsmodelpredictor_signature_start__
class StatsmodelPredictor(Predictor):
    ...
    # __statsmodelpredictor_signature_end__

    # __statsmodelpredictor_init_start__
    def __init__(self, results: Results, preprocessor: Optional[Preprocessor] = None):
        self.results = results
        super().__init__(preprocessor)
    # __statsmodelpredictor_init_end__

    # __statsmodelpredictor_predict_pandas_start__
    def _predict_pandas(self, data: pd.DataFrame) -> pd.DataFrame:
        predictions: pd.Series = self.results.predict(data)
        return predictions.to_frame(name="predictions")
    # __statsmodelpredictor_predict_pandas_end__

    # __statsmodelpredictor_from_checkpoint_start__
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
# __statsmodelpredictor_from_checkpoint_end__


# __statsmodelpredictor_model_start__
data: pd.DataFrame = sm.datasets.get_rdataset("Guerry", "HistData").data
results = smf.ols("Lottery ~ Literacy + np.log(Pop1831)", data=data).fit()
# __statsmodelpredictor_model_end__

# __statsmodelpredictor_checkpoint_start__
os.makedirs("checkpoint", exist_ok=True)
results.save("checkpoint/guerry.pickle")
checkpoint = Checkpoint.from_directory("checkpoint")
# __statsmodelpredictor_checkpoint_end__

# __statsmodelpredictor_predict_start__
predictor = BatchPredictor.from_checkpoint(
    checkpoint, StatsmodelPredictor, filename="guerry.pickle"
)
# This is the same data we trained our model on. Don't do this in practice.
dataset = ray.data.from_pandas(data)
predictor.predict(dataset)
# __statsmodelpredictor_predict_end__
# fmt: on

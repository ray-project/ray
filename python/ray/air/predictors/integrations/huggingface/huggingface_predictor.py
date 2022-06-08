from typing import Optional, Type, Union, List

import numpy as np
import pandas as pd

from transformers.pipelines import Pipeline, pipeline as pipeline_factory
from transformers.pipelines.table_question_answering import (
    TableQuestionAnsweringPipeline,
)

from ray.air.predictor import DataBatchType, Predictor
from ray.air.preprocessor import Preprocessor
from ray.air.checkpoint import Checkpoint
from ray.air._internal.checkpointing import load_preprocessor_from_dir


class HuggingFacePredictor(Predictor):
    """A predictor for HuggingFace Transformers PyTorch models.

    This predictor uses Transformers Pipelines for inference.

    Args:
        pipeline: The Transformers pipeline to use for inference.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self,
        pipeline: Optional[Pipeline] = None,
        preprocessor: Optional[Preprocessor] = None,
    ):
        self.pipeline = pipeline
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        *,
        pipeline: Optional[Type[Pipeline]] = None,
        **pipeline_kwargs,
    ) -> "HuggingFacePredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``HuggingFaceTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``HuggingFaceTrainer`` run.
            pipeline: A ``transformers.pipelines.Pipeline`` class to use.
                If not specified, will use the ``pipeline`` abstraction
                wrapper.
            **pipeline_kwargs: Any kwargs to pass to the pipeline
                initialization. If ``pipeline`` is None, this must contain
                the 'task' argument. Cannot contain 'model'.
        """
        if not pipeline and "task" not in pipeline_kwargs:
            raise ValueError(
                "If `pipeline` is not specified, 'task' must be passed as a kwarg."
            )
        pipeline = pipeline or pipeline_factory
        with checkpoint.as_directory() as checkpoint_path:
            preprocessor = load_preprocessor_from_dir(checkpoint_path)
            pipeline = pipeline(model=checkpoint_path, **pipeline_kwargs)
        return HuggingFacePredictor(
            pipeline=pipeline,
            preprocessor=preprocessor,
        )

    def _predict(
        self, data: Union[list, pd.DataFrame], **pipeline_call_kwargs
    ) -> pd.DataFrame:
        ret = self.pipeline(data, **pipeline_call_kwargs)
        # Remove unnecessary lists
        try:
            new_ret = [x[0] if isinstance(x, list) and len(x) == 1 else x for x in ret]
            df = pd.DataFrame(new_ret)
        except Exception:
            # if we fail for any reason, just give up
            df = pd.DataFrame(ret)
        df.columns = [str(col) for col in df.columns]
        return df

    def _convert_data_for_pipeline(
        self, data: pd.DataFrame
    ) -> Union[list, pd.DataFrame]:
        """Convert the data into a format accepted by the pipeline.

        In most cases, this format is a list of strings."""
        # Special case
        if isinstance(self.pipeline, TableQuestionAnsweringPipeline):
            return data
        # Otherwise, a list of columns as lists
        columns = [data[col].to_list() for col in data.columns]
        # Flatten if it's only one column
        if len(columns) == 1:
            columns = columns[0]
        return columns

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[List[str]] = None,
        **pipeline_call_kwargs,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into a list (unless ``pipeline`` is a
        ``TableQuestionAnsweringPipeline``) and passed to the ``pipeline``
        object.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, use all
                columns.
            **pipeline_call_kwargs: additional kwargs to pass to the
                ``pipeline`` object.

        Examples:

        .. code-block:: python

            import pandas as pd
            from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer
            from transformers.pipelines import pipeline
            from ray.air.predictors.integrations.huggingface import HuggingFacePredictor

            model_checkpoint = "gpt2"
            tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"
            tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)

            model_config = AutoConfig.from_pretrained(model_checkpoint)
            model = AutoModelForCausalLM.from_config(model_config)
            predictor = HuggingFacePredictor(
                pipeline=pipeline(
                    task="text-generation", model=model, tokenizer=tokenizer
                )
            )

            prompts = pd.DataFrame(
                ["Complete me", "And me", "Please complete"], columns=["sentences"]
            )
            predictions = predictor.predict(prompts)


        Returns:
            DataBatchType: Prediction result.
        """
        if self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        if isinstance(data, np.ndarray):
            # If numpy array, then convert to pandas dataframe.
            data = pd.DataFrame(data)

        data = data[feature_columns] if feature_columns else data

        data = self._convert_data_for_pipeline(data)
        return self._predict(data, **pipeline_call_kwargs)

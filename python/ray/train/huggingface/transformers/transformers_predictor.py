import logging
from typing import TYPE_CHECKING, List, Optional, Type, Union

import pandas as pd

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.data_batch_type import DataBatchType
from ray.train.predictor import Predictor
from ray.util import log_once
from ray.util.annotations import PublicAPI

try:
    import torch

    torch_get_gpus = torch.cuda.device_count
except ImportError:

    def torch_get_gpus():
        return 0


try:
    import tensorflow

    def tf_get_gpus():
        return len(tensorflow.config.list_physical_devices("GPU"))

except ImportError:

    def tf_get_gpus():
        return 0


TRANSFORMERS_IMPORT_ERROR: Optional[ImportError] = None
try:
    from transformers.pipelines import Pipeline
    from transformers.pipelines import pipeline as pipeline_factory
    from transformers.pipelines.table_question_answering import (
        TableQuestionAnsweringPipeline,
    )
except ImportError as e:
    TRANSFORMERS_IMPORT_ERROR = e


if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor
    from ray.train.huggingface import TransformersCheckpoint
    from transformers.modeling_utils import PreTrainedModel
    from transformers.modeling_tf_utils import TFPreTrainedModel

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class TransformersPredictor(Predictor):
    """A predictor for HuggingFace Transformers PyTorch models.

    This predictor uses Transformers Pipelines for inference.

    Args:
        pipeline: The Transformers pipeline to use for inference.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
        use_gpu: If set, the model will be moved to GPU on instantiation and
            prediction happens on GPU.
    """

    def __init__(
        self,
        pipeline: Optional["Pipeline"] = None,
        preprocessor: Optional["Preprocessor"] = None,
        use_gpu: bool = False,
    ):

        if TRANSFORMERS_IMPORT_ERROR is not None:
            raise TRANSFORMERS_IMPORT_ERROR

        self.pipeline = pipeline
        self.use_gpu = use_gpu

        num_gpus = max(torch_get_gpus(), tf_get_gpus())
        if not use_gpu and num_gpus > 0 and log_once("hf_predictor_not_using_gpu"):
            logger.warning(
                "You have `use_gpu` as False but there are "
                f"{num_gpus} GPUs detected on host where "
                "prediction will only use CPU. Please consider explicitly "
                "setting `TransformersPredictor(use_gpu=True)` or "
                "`batch_predictor.predict(ds, num_gpus_per_worker=1)` to "
                "enable GPU prediction. Ignore if you have set `device` or "
                "`device_map` arguments in the `pipeline` manually."
            )

        super().__init__(preprocessor)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(pipeline={self.pipeline!r}, "
            f"preprocessor={self._preprocessor!r})"
        )

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: "TransformersCheckpoint",
        *,
        pipeline_cls: Optional[Type["Pipeline"]] = None,
        model_cls: Optional[
            Union[str, Type["PreTrainedModel"], Type["TFPreTrainedModel"]]
        ] = None,
        pretrained_model_kwargs: Optional[dict] = None,
        use_gpu: bool = False,
        **pipeline_kwargs,
    ) -> "TransformersPredictor":
        """Instantiate the predictor from a TransformersCheckpoint.

        Note that the Transformers ``pipeline`` used internally expects to
        receive raw text. If you have any Preprocessors in Checkpoint
        that tokenize the data, remove them by calling
        ``Checkpoint.set_preprocessor(None)`` beforehand.

        Args:
            checkpoint: The checkpoint to load the model, tokenizer and
                preprocessor from.
            pipeline_cls: A ``transformers.pipelines.Pipeline`` class to use.
                If not specified, will use the ``pipeline`` abstraction
                wrapper.
            model_cls: A ``transformers.PreTrainedModel`` class to create from
                the checkpoint.
            pretrained_model_kwargs: If set and a ``model_cls`` is provided, will
                be passed to ``TransformersCheckpoint.get_model()``.
            use_gpu: If set, the model will be moved to GPU on instantiation and
                prediction happens on GPU.
            **pipeline_kwargs: Any kwargs to pass to the pipeline
                initialization. If ``pipeline_cls`` is None, this must contain
                the 'task' argument. Can be used
                to override the tokenizer with 'tokenizer'. If ``use_gpu`` is
                True, 'device' will be set to 0 by default, unless 'device_map' is
                passed.
        """
        if TRANSFORMERS_IMPORT_ERROR is not None:
            raise TRANSFORMERS_IMPORT_ERROR

        if not pipeline_cls and "task" not in pipeline_kwargs:
            raise ValueError(
                "If `pipeline_cls` is not specified, 'task' must be passed as a kwarg."
            )
        if use_gpu and "device_map" not in pipeline_kwargs:
            # default to using the GPU with the first index
            pipeline_kwargs.setdefault("device", 0)

        model = None
        if model_cls:
            pretrained_model_kwargs = pretrained_model_kwargs or {}
            model = checkpoint.get_model(model_cls, **pretrained_model_kwargs)

        if pipeline_cls and model:
            # Custom pipeline is passed and model was retrieved
            pipeline = pipeline_cls(model, **pipeline_kwargs)
        else:
            # Custom pipeline class
            if pipeline_cls:
                pipeline_kwargs["pipeline_class"] = pipeline_cls

            if not model:
                # Infer model from checkpoint
                with checkpoint.as_directory() as checkpoint_path:
                    # Tokenizer will be loaded automatically (no need to specify
                    # `tokenizer=checkpoint_path`)
                    pipeline = pipeline_factory(
                        model=checkpoint_path, **pipeline_kwargs
                    )
            else:
                # Use model with default pipeline
                pipeline = pipeline_factory(model=model, **pipeline_kwargs)

        preprocessor = checkpoint.get_preprocessor()

        return cls(
            pipeline=pipeline,
            preprocessor=preprocessor,
            use_gpu=use_gpu,
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

    @staticmethod
    def _convert_data_for_pipeline(
        data: pd.DataFrame, pipeline: "Pipeline"
    ) -> Union[list, pd.DataFrame]:
        """Convert the data into a format accepted by the pipeline.

        In most cases, this format is a list of strings."""
        # Special case where pd.DataFrame is allowed.
        if isinstance(pipeline, TableQuestionAnsweringPipeline):
            # TODO(team-ml): This may be a performance bottleneck.
            return data

        # Otherwise, a list of columns as lists.
        columns = [data[col].to_list() for col in data.columns]
        # Flatten if it's only one column.
        while isinstance(columns, list) and len(columns) == 1:
            columns = columns[0]
        return columns

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        **predict_kwargs,
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
            >>> import pandas as pd
            >>> from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer
            >>> from transformers.pipelines import pipeline
            >>> from ray.train.huggingface import TransformersPredictor
            >>>
            >>> model_checkpoint = "gpt2"
            >>> tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"
            >>> tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)
            >>>
            >>> model_config = AutoConfig.from_pretrained(model_checkpoint)
            >>> model = AutoModelForCausalLM.from_config(model_config)
            >>> predictor = TransformersPredictor(
            ...     pipeline=pipeline(
            ...         task="text-generation", model=model, tokenizer=tokenizer
            ...     )
            ... )
            >>>
            >>> prompts = pd.DataFrame(
            ...     ["Complete me", "And me", "Please complete"], columns=["sentences"]
            ... )
            >>> predictions = predictor.predict(prompts)


        Returns:
            Prediction result.
        """
        return Predictor.predict(
            self, data, feature_columns=feature_columns, **predict_kwargs
        )

    def _predict_pandas(
        self,
        data: "pd.DataFrame",
        feature_columns: Optional[List[str]] = None,
        **pipeline_call_kwargs,
    ) -> "pd.DataFrame":
        if TENSOR_COLUMN_NAME in data:
            arr = data[TENSOR_COLUMN_NAME].to_numpy()
            if feature_columns:
                data = pd.DataFrame(arr[:, feature_columns])
        elif feature_columns:
            data = data[feature_columns]

        data = data[feature_columns] if feature_columns else data

        data = self._convert_data_for_pipeline(data, self.pipeline)
        return self._predict(data, **pipeline_call_kwargs)

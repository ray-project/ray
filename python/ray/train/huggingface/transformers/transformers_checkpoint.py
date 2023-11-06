import os
import tempfile
from typing import TYPE_CHECKING, Optional, Type, Union

import torch

from ray.air._internal.torch_utils import load_torch_model
from ray.train._internal.framework_checkpoint import FrameworkCheckpoint
from ray.util.annotations import Deprecated

TRANSFORMERS_IMPORT_ERROR: Optional[ImportError] = None

try:
    import transformers
    import transformers.modeling_utils
    import transformers.trainer
    import transformers.training_args
    from transformers.trainer import TRAINING_ARGS_NAME, WEIGHTS_NAME
except ImportError as e:
    TRANSFORMERS_IMPORT_ERROR = e

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

TRANSFORMERS_CHECKPOINT_DEPRECATION_MESSAGE = (
    "`TransformersCheckpoint` will be hard deprecated in Ray 2.8. Please use "
    "`ray.train.Checkpoint` instead."
)


@Deprecated
class TransformersCheckpoint(FrameworkCheckpoint):
    """A :py:class:`~ray.train.Checkpoint` with HuggingFace-specific functionality.

    Use ``TransformersCheckpoint.from_model`` to create this type of checkpoint.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raise DeprecationWarning(TRANSFORMERS_CHECKPOINT_DEPRECATION_MESSAGE)

    @classmethod
    def from_model(
        cls,
        model: Union["transformers.modeling_utils.PreTrainedModel", "torch.nn.Module"],
        tokenizer: Optional["transformers.PreTrainedTokenizer"] = None,
        *,
        path: Union[str, os.PathLike] = None,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "TransformersCheckpoint":
        """Create a :py:class:`~ray.train.Checkpoint` that stores a HuggingFace model.

        Args:
            model: The pretrained transformer or Torch model to store in the
                checkpoint.
            tokenizer: The Tokenizer to use in the Transformers pipeline for inference.
            path: The directory where the checkpoint will be stored.
                Defaults to a temp directory.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            A :py:class:`TransformersCheckpoint` containing the specified model.
        """

        if TRANSFORMERS_IMPORT_ERROR is not None:
            raise TRANSFORMERS_IMPORT_ERROR

        path = path or tempfile.mkdtemp()

        if not isinstance(model, transformers.modeling_utils.PreTrainedModel):
            state_dict = model.state_dict()
            torch.save(state_dict, os.path.join(path, WEIGHTS_NAME))
        else:
            model.save_pretrained(path)

        if tokenizer:
            tokenizer.save_pretrained(path)

        checkpoint = cls.from_directory(path)
        if preprocessor:
            checkpoint.set_preprocessor(preprocessor)

        return checkpoint

    def get_model(
        self,
        model: Union[
            Type["transformers.modeling_utils.PreTrainedModel"], "torch.nn.Module"
        ],
        **pretrained_model_kwargs,
    ) -> Union["transformers.modeling_utils.PreTrainedModel", "torch.nn.Module"]:
        """Retrieve the model stored in this checkpoint."""
        with self.as_directory() as checkpoint_path:
            if isinstance(model, torch.nn.Module):
                state_dict = torch.load(
                    os.path.join(checkpoint_path, WEIGHTS_NAME), map_location="cpu"
                )
                model = load_torch_model(saved_model=state_dict, model_definition=model)
            else:
                model = model.from_pretrained(
                    checkpoint_path, **pretrained_model_kwargs
                )
        return model

    def get_tokenizer(
        self,
        tokenizer: Type["transformers.PreTrainedTokenizer"],
        **kwargs,
    ) -> Optional["transformers.PreTrainedTokenizer"]:
        """Create a tokenizer using the data stored in this checkpoint."""
        with self.as_directory() as checkpoint_path:
            return tokenizer.from_pretrained(checkpoint_path, **kwargs)

    def get_training_arguments(self) -> "transformers.training_args.TrainingArguments":
        """Retrieve the training arguments stored in this checkpoint."""
        with self.as_directory() as checkpoint_path:
            training_args_path = os.path.join(checkpoint_path, TRAINING_ARGS_NAME)
            if os.path.exists(training_args_path):
                with open(training_args_path, "rb") as f:
                    training_args = torch.load(f, map_location="cpu")
            else:
                training_args = None
        return training_args

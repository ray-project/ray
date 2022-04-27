import os
from typing import Tuple, Type, Union

import torch
from transformers.modeling_utils import PreTrainedModel
from transformers.trainer import WEIGHTS_NAME, TRAINING_ARGS_NAME
from transformers import TrainingArguments

import ray.cloudpickle as cpickle
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.torch_utils import load_torch_model
from ray.ml.constants import PREPROCESSOR_KEY
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def load_huggingface_checkpoint(
    checkpoint: Checkpoint,
    model: Union[Type[PreTrainedModel], torch.nn.Module],
    **pretrained_model_kwargs
) -> Tuple[Union[PreTrainedModel, torch.nn.Module], Preprocessor, TrainingArguments]:
    """Load a Checkpoint from ``HuggingFaceTrainer`` and return the
    model, preprocessor and ``TrainingArguments`` contained within.

    Args:
        checkpoint: The checkpoint to load the model and
            preprocessor from. It is expected to be from the result of a
            ``HuggingFaceTrainer`` run.
        model: Either a ``transformers.PreTrainedModel`` class
            (eg. ``AutoModelForCausalLM``), or a PyTorch model to load the
            weights to. This should be the same model used for training.
    """
    with checkpoint.as_directory() as checkpoint_path:
        preprocessor_path = os.path.join(checkpoint_path, PREPROCESSOR_KEY)
        if os.path.exists(preprocessor_path):
            with open(preprocessor_path, "rb") as f:
                preprocessor = cpickle.load(f)
        else:
            preprocessor = None
        if isinstance(model, torch.nn.Module):
            state_dict = torch.load(
                os.path.join(checkpoint_path, WEIGHTS_NAME), map_location="cpu"
            )
            model = load_torch_model(saved_model=state_dict, model_definition=model)
        else:
            model = model.from_pretrained(checkpoint_path, **pretrained_model_kwargs)
        training_args_path = os.path.join(checkpoint_path, TRAINING_ARGS_NAME)
        if os.path.exists(training_args_path):
            with open(training_args_path, "rb") as f:
                training_args = torch.load(f, map_location="cpu")
        else:
            training_args = None
    return model, preprocessor, training_args

import os
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Type, Union

import torch
import transformers
import transformers.modeling_utils
import transformers.trainer
import transformers.training_args
from transformers.trainer import TRAINING_ARGS_NAME, WEIGHTS_NAME

from ray.air._internal.checkpointing import (
    load_preprocessor_from_dir,
    save_preprocessor_to_dir,
)
from ray.air._internal.torch_utils import load_torch_model
from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
def to_air_checkpoint(
    model: Union[transformers.modeling_utils.PreTrainedModel, torch.nn.Module],
    tokenizer: Optional[transformers.PreTrainedTokenizer] = None,
    *,
    path: os.PathLike,
    preprocessor: Optional["Preprocessor"] = None,
) -> Checkpoint:
    """Convert a pretrained Transformers model to AIR checkpoint for serve or inference.

    Example:

    .. code-block:: python

        import tempfile
        from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer
        from ray.train.huggingface import to_air_checkpoint, HuggingFacePredictor

        model_checkpoint = "sshleifer/tiny-gpt2"
        tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"

        model_config = AutoConfig.from_pretrained(model_checkpoint)
        model = AutoModelForCausalLM.from_config(model_config)
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)

        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint = to_air_checkpoint(
                model=model, tokenizer=tokenizer, path=tmpdir
            )
            predictor = HuggingFacePredictor.from_checkpoint(checkpoint)

    Args:
        model: Either a ``transformers.PreTrainedModel``, or a trained PyTorch model.
        path: The directory where the checkpoint will be stored to.
        tokenizer: Tokenizer to be used in the Transformers pipeline
            during serving/inference.
        preprocessor: A fitted preprocessor. The preprocessing logic will
            be applied to the inputs for serving/inference.

    Returns:
        A Ray AIR checkpoint.

    """
    if not isinstance(model, transformers.modeling_utils.PreTrainedModel):
        state_dict = model.state_dict()
        torch.save(state_dict, os.path.join(path, WEIGHTS_NAME))
    else:
        model.save_pretrained(path)
    if tokenizer:
        tokenizer.save_pretrained(path)
    if preprocessor:
        save_preprocessor_to_dir(preprocessor, path)
    checkpoint = Checkpoint.from_directory(path)

    return checkpoint


@PublicAPI(stability="alpha")
def load_checkpoint(
    checkpoint: Checkpoint,
    model: Union[Type[transformers.modeling_utils.PreTrainedModel], torch.nn.Module],
    tokenizer: Optional[Type[transformers.PreTrainedTokenizer]] = None,
    *,
    tokenizer_kwargs: Optional[Dict[str, Any]] = None,
    **pretrained_model_kwargs,
) -> Tuple[
    Union[transformers.modeling_utils.PreTrainedModel, torch.nn.Module],
    transformers.training_args.TrainingArguments,
    Optional[transformers.PreTrainedTokenizer],
    Optional["Preprocessor"],
]:
    """Load a Checkpoint from ``HuggingFaceTrainer``.


    Args:
        checkpoint: The checkpoint to load the model and
            preprocessor from. It is expected to be from the result of a
            ``HuggingFaceTrainer`` run.
        model: Either a ``transformers.PreTrainedModel`` class
            (eg. ``AutoModelForCausalLM``), or a PyTorch model to load the
            weights to. This should be the same model used for training.
        tokenizer: A ``transformers.PreTrainedTokenizer`` class to load
            the model tokenizer to. If not specified, the tokenizer will
            not be loaded. Will throw an exception if specified, but no
            tokenizer was found in the checkpoint.
        tokenizer_kwargs: Dict of kwargs to pass to ``tokenizer.from_pretrained``
            call. Ignored if ``tokenizer`` is None.
        **pretrained_model_kwargs: Kwargs to pass to ``mode.from_pretrained``
            call. Ignored if ``model`` is not a ``transformers.PreTrainedModel``
            class.

    Returns:
        The model, ``TrainingArguments``, tokenizer and AIR preprocessor
        contained within. Those can be used to initialize a ``transformers.Trainer``
        object locally.
    """
    tokenizer_kwargs = tokenizer_kwargs or {}
    with checkpoint.as_directory() as checkpoint_path:
        preprocessor = load_preprocessor_from_dir(checkpoint_path)
        if isinstance(model, torch.nn.Module):
            state_dict = torch.load(
                os.path.join(checkpoint_path, WEIGHTS_NAME), map_location="cpu"
            )
            model = load_torch_model(saved_model=state_dict, model_definition=model)
        else:
            model = model.from_pretrained(checkpoint_path, **pretrained_model_kwargs)
        if tokenizer:
            tokenizer = tokenizer.from_pretrained(checkpoint_path, **tokenizer_kwargs)
        training_args_path = os.path.join(checkpoint_path, TRAINING_ARGS_NAME)
        if os.path.exists(training_args_path):
            with open(training_args_path, "rb") as f:
                training_args = torch.load(f, map_location="cpu")
        else:
            training_args = None
    return model, training_args, tokenizer, preprocessor

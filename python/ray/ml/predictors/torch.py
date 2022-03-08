from typing import Optional, Union, Dict

import pandas as pd
import torch

import ray
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor
from ray.ml.preprocessor import Preprocessor

from ray.ml.constants import PREPROCESSOR_KEY
from ray.ml.constants import MODEL_KEY


class TorchPredictor(Predictor):
    """A predictor for PyTorch models."""

    def __init__(self, model: torch.nn.Module, preprocessor: Preprocessor):
        self.model = model
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, model_definition: Optional[torch.nn.Module] = None
    ) -> Predictor:
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``TorchTrainer``.

        Args:
            checkpoint (Checkpoint): The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``TorchTrainer`` run.
            model_definition (Optional[torch.nn.Module]): If checkpoints
                contains the model state dict, and not the model itself,
                then the state dict will be loaded to the model_definition.

        """
        checkpoint_dict = checkpoint.to_dict()
        preprocessor = checkpoint_dict[PREPROCESSOR_KEY]
        model = load_torch_model(
            saved_model=checkpoint_dict[MODEL_KEY], model_definition=model_definition
        )
        return TorchPredictor(model=model, preprocessor=preprocessor)

    def predict(
        self, preprocessed_data: ray.data.Dataset, **to_torch_kwargs
    ) -> ray.data.Dataset:
        predictions = []
        for batch in preprocessed_data.to_torch(**to_torch_kwargs):
            features = batch[0]
            predictions.append(self.score_fn(features))
        return pd.DataFrame({"predictions": predictions})

    def score_fn(self, data):
        return self.model(data)


# TODO: Find a better place for this.
def load_torch_model(
    saved_model: Union[torch.nn.Module, Dict],
    model_definition: Optional[torch.nn.Module] = None,
) -> torch.nn.Module:
    if isinstance(saved_model, torch.nn.Module):
        return saved_model
    elif isinstance(saved_model, dict):
        if not model_definition:
            raise RuntimeError(
                "Attempting to load torch model from a "
                "state_dict, but no `model_definition` was "
                "provided."
            )
        model_definition.load_state_dict(saved_model)
        return model_definition
    else:
        raise RuntimeError(
            f"Saved model is of type {type(saved_model)}. "
            f"The model saved in the checkpoint is expected "
            f"to be of type `torch.nn.Module`, or a model "
            f"state dict of type dict."
        )

import torch

from ray.air.checkpoint import Checkpoint


class TorchCheckpoint(Checkpoint):

    @staticmethod
    def from_torch_model(
        model: torch.nn.Module, *, preprocessor: Optional["Preprocessor"] = None
    ) -> Checkpoint:
        """Create a Torch trainer checkpoint from a torch module.

        Args:
            model: A pretrained model.
            preprocessor: A fitted preprocessor. The preprocessing logic will
                be applied to the inputs for serving/inference.

        Returns:
            A checkpoint that can be loaded by TorchPredictor.
        """
        checkpoint = Checkpoint.from_dict(
            {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model}
        )
        return checkpoint

    def get_model(self) -> torch.nn.Module:
        """Return the torch model contained in this Checkpoint."""
        return self.as_dict()[MODEL_KEY]

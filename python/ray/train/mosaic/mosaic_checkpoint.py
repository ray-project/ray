from ray.air.checkpoint import Checkpoint
import torch


class MosaicCheckpoint(Checkpoint):
    def get_model(
        self,
        model: torch.nn.Module,
        strict: bool = False,
    ) -> torch.nn.Module:
        """Retrieve the model stored in this checkpoint."""
        checkpoint_dict = self.to_dict()
        model_state_dict = checkpoint_dict["state"]["model"]
        torch.nn.modules.utils.consume_prefix_in_state_dict_if_present(
            model_state_dict, "module."
        )
        model.load_state_dict(model_state_dict, strict=strict)
        return model

    @classmethod
    def from_directory(cls, path: str):
        composer_state_dict = torch.load(path)
        return super().from_dict(composer_state_dict)

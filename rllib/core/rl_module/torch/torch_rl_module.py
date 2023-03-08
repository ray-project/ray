import pathlib
from typing import Any, Mapping

from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchRLModule(nn.Module, RLModule):
    def __init__(self, *args, **kwargs) -> None:
        nn.Module.__init__(self)
        RLModule.__init__(self, *args, **kwargs)

    def forward(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """forward pass of the module.

        This is aliased to forward_train because Torch DDP requires a forward method to
        be implemented for backpropagation to work.
        """
        return self.forward_train(batch, **kwargs)

    @override(RLModule)
    def get_state(self) -> Mapping[str, Any]:
        return self.state_dict()

    @override(RLModule)
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        self.load_state_dict(state_dict)

    @override(RLModule)
    def save_to_checkpoint(self, checkpoint_dir_path: str) -> None:
        """Saves the module to a checkpoint directory.

        Args:
            dir_path: The directory to save the checkpoint to.


        Raises:
            ValueError: If dir_path is not an absolute path.
        """
        path = pathlib.Path(checkpoint_dir_path)
        if not path.is_absolute():
            raise ValueError("dir_path must be an absolute path.")
        path.mkdir(parents=True, exist_ok=True)
        module_state_path = path / "module_state.pt"
        torch.save(self.state_dict(), str(module_state_path))
        self._save_module_metadata(path, module_state_path)

    @classmethod
    def load_from_checkpoint(cls, checkpoint_dir_path: str) -> "TorchRLModule":
        """Loads the module from a checkpoint directory.

        Args:
            dir_path: The directory to load the checkpoint from.
        """
        path = pathlib.Path(checkpoint_dir_path)
        if not path.exists():
            raise ValueError(
                "While loading from checkpoint there was no directory"
                " found at {}".format(checkpoint_dir_path)
            )
        if not path.is_absolute():
            raise ValueError("dir_path must be an absolute path.")
        if not path.is_dir():
            raise ValueError(
                "While loading from checkpoint the checkpoint_dir_path "
                "provided was not a directory."
            )

        module = cls._from_metadata_file(path)
        state_path = path / "module_state.pt"
        if not state_path.exists():
            raise ValueError(
                "While loading from checkpoint there was no module_state.pt"
                " found at {}".format(checkpoint_dir_path)
            )
        module.set_state(torch.load(str(state_path)))
        return module

    @override(RLModule)
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        """Makes the module distributed."""
        # TODO (Avnish): Implement this.
        pass

    @override(RLModule)
    def is_distributed(self) -> bool:
        """Returns True if the module is distributed."""
        # TODO (Avnish): Implement this.
        return False


class TorchDDPRLModule(RLModule, nn.parallel.DistributedDataParallel):
    def __init__(self, *args, **kwargs) -> None:
        nn.parallel.DistributedDataParallel.__init__(self, *args, **kwargs)
        # we do not want to call RLModule.__init__ here because all we need is
        # the interface of that base-class not the actual implementation.

    @override(RLModule)
    def _forward_train(self, *args, **kwargs):
        return self(*args, **kwargs)

    @override(RLModule)
    def _forward_inference(self, *args, **kwargs) -> Mapping[str, Any]:
        return self.module._forward_inference(*args, **kwargs)

    @override(RLModule)
    def _forward_exploration(self, *args, **kwargs) -> Mapping[str, Any]:
        return self.module._forward_exploration(*args, **kwargs)

    @override(RLModule)
    def get_state(self, *args, **kwargs):
        return self.module.get_state(*args, **kwargs)

    @override(RLModule)
    def set_state(self, *args, **kwargs):
        self.module.set_state(*args, **kwargs)

    @override(RLModule)
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        # TODO (Kourosh): Not to sure about this make_distributed api belonging to
        # RLModule or the Learner? For now the logic is kept in Learner.
        # We should see if we can use this api end-point for both tf
        # and torch instead of doing it in the learner.
        pass

    @override(RLModule)
    def is_distributed(self) -> bool:
        return True

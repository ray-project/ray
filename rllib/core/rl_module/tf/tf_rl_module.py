import pathlib
from typing import Any, Mapping

from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf


_, tf, _ = try_import_tf()


class TfRLModule(RLModule, tf.keras.Model):
    """Base class for RLlib TF RLModules."""

    def __init__(self, *args, **kwargs) -> None:
        tf.keras.Model.__init__(self)
        RLModule.__init__(self, *args, **kwargs)

    @override(tf.keras.Model)
    def call(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """Forward pass of the module.

        Note:
            This is aliased to forward_train to follow the Keras Model API.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_train().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_train().

        """
        return self.forward_train(batch)

    @override(RLModule)
    def get_state(self) -> Mapping[str, Any]:
        return self.get_weights()

    @override(RLModule)
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        self.set_weights(state_dict)

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
        module_state_dir = path / "module_state"
        module_state_dir.mkdir(parents=True, exist_ok=True)
        module_state_path = module_state_dir / "module_state"
        self.save_weights(str(module_state_path), save_format="tf")
        self._save_module_metadata(path, module_state_path)

    @classmethod
    def load_from_checkpoint(cls, checkpoint_dir_path: str) -> "TfRLModule":
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
        module_state_dir = path / "module_state"
        if not module_state_dir.exists():
            raise ValueError(
                "While loading from checkpoint there was no 'module_state'"
                " directory found at {}".format(checkpoint_dir_path)
            )
        module_state_dir.mkdir(parents=True, exist_ok=True)
        module_state_path = module_state_dir / "module_state"
        module.load_weights(module_state_path)
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

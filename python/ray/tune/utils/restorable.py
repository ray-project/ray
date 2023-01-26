import abc
import glob
import os
from typing import Dict, Optional

from ray.tune.utils.util import _atomic_save, _load_newest_checkpoint


class Restorable(abc.ABC):
    """Interface for an object that can save and restore state to a directory.

    The object's state will be saved as a file of the form `CKPT_FILE_TMPL`.
    When implementing this interface, be sure to change `CKPT_FILE_TMPL` to
    something unique to the object being stored.
    """

    CKPT_FILE_TMPL = "restorable-state-{}.pkl"

    def get_state(self) -> Optional[Dict]:
        """Get the state of the object.

        This method should be implemented by subclasses to return a dictionary
        representation of the object's current state.

        Returns:
            state: State of the object. Should be `None` if the object does not
                have any state to save.
        """
        raise NotImplementedError

    def set_state(self, state: Dict):
        """Get the state of the object.

        This method should be implemented by subclasses to restore the object's
        state based on the given dict state.

        Args:
            state: State of the object.
        """
        raise NotImplementedError

    def handle_save_error(self, error: Exception):
        """Handle error occurred during saving.

        For example, this can be used to log a warning if `get_state` isn't implemented.

        Args:
            error: The exception that occurred during saving.
        """
        pass

    def handle_restore_error(self, error: Exception):
        """Handle error occurred during restoring.

        For example, this can be used to log a warning if `set_state` isn't implemented.

        Args:
            error: The exception that occurred during restoring.
        """
        pass

    def can_restore(self, checkpoint_dir: str) -> bool:
        """Check if the checkpoint_dir contains the saved state for this object.

        Returns:
            can_restore: True if the checkpoint_dir contains a file of the
                format `CKPT_FILE_TMPL`. False otherwise.
        """
        return bool(
            glob.glob(os.path.join(checkpoint_dir, self.CKPT_FILE_TMPL.format("*")))
        )

    def save_to_dir(self, checkpoint_dir: str, session_str: str = "default"):
        """Save the state of the object to the checkpoint_dir.

        Args:
            checkpoint_dir: directory where the checkpoint is stored.
            session_str: Unique identifier of the current run session (ex: timestamp).

        Raises:
            NotImplementedError: if the `get_state` method is not implemented.
        """
        try:
            state_dict = self.get_state()
        except NotImplementedError as e:
            self.handle_save_error(e)
            state_dict = None

        if state_dict:
            file_name = self.CKPT_FILE_TMPL.format(session_str)
            tmp_file_name = f".tmp-{file_name}"
            try:
                _atomic_save(
                    state=state_dict,
                    checkpoint_dir=checkpoint_dir,
                    file_name=file_name,
                    tmp_file_name=tmp_file_name,
                )
            except Exception as e:
                self.handle_save_error(e)

    def restore_from_dir(self, checkpoint_dir: str):
        """Restore the state of the object from the checkpoint_dir.

        You should check if it's possible to restore with `can_restore`
        before calling this method.

        Args:
            checkpoint_dir: directory where the checkpoint is stored.

        Raises:
            RuntimeError: if unable to find checkpoint.
            NotImplementedError: if the `set_state` method is not implemented.
        """
        state_dict = _load_newest_checkpoint(
            checkpoint_dir, self.CKPT_FILE_TMPL.format("*")
        )
        if not state_dict:
            raise RuntimeError(
                "Unable to find checkpoint in {}.".format(checkpoint_dir)
            )
        try:
            self.set_state(state_dict)
        except Exception as e:
            self.handle_restore_error(e)

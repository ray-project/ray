import abc
import glob
import os
from typing import Dict, Optional

from ray.tune.utils.util import _atomic_save, _load_newest_checkpoint


class Restorable(abc.ABC):
    CKPT_FILE_TMPL = "restorable-state-{}.pkl"

    def get_state(self) -> Optional[Dict]:
        raise NotImplementedError

    def set_state(self, state: Dict):
        raise NotImplementedError

    def handle_save_error(self, error: Exception):
        pass

    def handle_save_error(self, error: Exception):
        pass

    def can_restore(self, checkpoint_dir: str) -> bool:
        return bool(
            glob.glob(os.path.join(checkpoint_dir, self.CKPT_FILE_TMPL.format("*")))
        )

    def save_to_dir(self, checkpoint_dir: str, session_str: str = "default"):
        """Automatically saves this object's state to the checkpoint_dir.

        Args:
            checkpoint_dir: Filepath to experiment dir.
            session_str: Unique identifier of the current run
                session (ex: timestamp).
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

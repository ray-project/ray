import os

from typing import List, Optional
from ray._private.runtime_env import virtualenv_utils


INTERNAL_PIP_FILENAME = "ray_runtime_env_internal_pip_requirements.txt"
MAX_INTERNAL_PIP_FILENAME_TRIES = 100


class PathHelper:
    @staticmethod
    def get_virtualenv_path(target_dir: str) -> str:
        return os.path.join(target_dir, "virtualenv")

    @classmethod
    def get_virtualenv_python(cls, target_dir: str) -> str:
        virtualenv_path = cls.get_virtualenv_path(target_dir)
        if virtualenv_utils._WIN32:
            return os.path.join(virtualenv_path, "Scripts", "python.exe")
        else:
            return os.path.join(virtualenv_path, "bin", "python")

    @classmethod
    def get_virtualenv_activate_command(cls, target_dir: str) -> List[str]:
        virtualenv_path = cls.get_virtualenv_path(target_dir)
        if virtualenv_utils._WIN32:
            cmd = [os.path.join(virtualenv_path, "Scripts", "activate.bat")]

        else:
            cmd = ["source", os.path.join(virtualenv_path, "bin/activate")]
        return cmd + ["1>&2", "&&"]

    @staticmethod
    def get_requirements_file(target_dir: str, pip_list: Optional[List[str]]) -> str:
        """Returns the path to the requirements file to use for this runtime env.

        If pip_list is not None, we will check if the internal pip filename is in any of
        the entries of pip_list. If so, we will append numbers to the end of the
        filename until we find one that doesn't conflict. This prevents infinite
        recursion if the user specifies the internal pip filename in their pip list.

        Args:
            target_dir: The directory to store the requirements file in.
            pip_list: A list of pip requirements specified by the user.

        Returns:
            The path to the requirements file to use for this runtime env.
        """

        def filename_in_pip_list(filename: str) -> bool:
            for pip_entry in pip_list:
                if filename in pip_entry:
                    return True
            return False

        filename = INTERNAL_PIP_FILENAME
        if pip_list is not None:
            i = 1
            while (
                filename_in_pip_list(filename) and i < MAX_INTERNAL_PIP_FILENAME_TRIES
            ):
                filename = f"{INTERNAL_PIP_FILENAME}.{i}"
                i += 1
            if i == MAX_INTERNAL_PIP_FILENAME_TRIES:
                raise RuntimeError(
                    "Could not find a valid filename for the internal "
                    "pip requirements file. Please specify a different "
                    "pip list in your runtime env."
                )
        return os.path.join(target_dir, filename)

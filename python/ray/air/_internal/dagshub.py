import os
import glob
from pathlib import Path
from typing import Optional
from copy import deepcopy

from .mlflow import _MLflowLoggerUtil

try:
    from dagshub.upload import Repo
except Exception:
    Repo = None


class _DagsHubLoggerUtil(_MLflowLoggerUtil):
    def __init__(
        self, repo_owner: str = "", repo_name: str = "", mlflow_only: bool = False
    ):
        super(_DagsHubLoggerUtil, self).__init__()

        if Repo:
            self.mlflow_only = mlflow_only
            self.repo = Repo(
                owner=repo_owner,
                name=repo_name,
                branch=os.getenv("BRANCH", "main"),
            )

            self.paths = {
                "dvc_directory": Path("ray_artifacts"),
            }

            if not mlflow_only:
                self.dvc_folder = self.repo.directory(str(self.paths["dvc_directory"]))

    def __deepcopy__(self, memo=None):
        # mlflow is a module, and thus cannot be copied
        _mlflow = self._mlflow
        self.__dict__.pop("_mlflow")
        dict_copy = deepcopy(self.__dict__, memo)
        copied_object = _DagsHubLoggerUtil()
        copied_object.__dict__.update(dict_copy)
        self._mlflow = _mlflow
        copied_object._mlflow = _mlflow
        return copied_object

    def _dvc_add(self, local_path="", remote_path: str = ""):
        if not os.path.isfile(local_path):
            raise FileNotFoundError(
                f"The file is not exist!, currently recieved path: {local_path}"
            )
        self.dvc_folder.add(file=local_path, path=remote_path)

    def _dvc_commit(self, dvc_fld=None, commit=""):
        dvc_fld.commit(commit, versioning="dvc", force=True)

    def upload(self, run_id: str = ""):
        self._dvc_commit(
            self.dvc_folder, commit=f"ray.io: Add all artifacts from run {run_id}"
        )

    def save_artifacts(
        self, dir: str, run_id: Optional[str] = None, remote_path: str = "artifacts"
    ):
        """Saves directory as artifact to the run specified by run_id.

        If no ``run_id`` is passed in, then saves to the current active run.
        If there is not active run, then creates a new run and sets it as
        the active run.

        Args:
            dir: Path to directory containing the files to save.
            run_id (Optional[str]): The ID of the run to log to.
        """
        filelist = glob.glob(os.path.join(dir, "**", "*"), recursive=True)

        if self.mlflow_only:
            if run_id and self._run_exists(run_id):
                client = self._get_client()
                client.log_artifacts(run_id=run_id, local_dir=dir)
            else:
                self._mlflow.log_artifacts(local_dir=dir)
        else:
            for file in filelist:
                if not os.path.isdir(file):
                    remote_path = file.replace(dir + "/", "")
                    if run_id:
                        remote_path = os.path.join(run_id, remote_path)
                    else:
                        remote_path = os.path.join(file.split(os.sep)[-2], remote_path)
                    self._dvc_add(file, remote_path=remote_path)

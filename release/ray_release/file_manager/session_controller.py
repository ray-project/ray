import os
from typing import Optional

from anyscale.controllers.session_controller import SessionController

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.file_manager.file_manager import FileManager
from ray_release.logger import logger


class SessionControllerFileManager(FileManager):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        session_controller: Optional[SessionController] = None,
    ):
        super(SessionControllerFileManager, self).__init__(cluster_manager)
        self.session_controller = session_controller or SessionController()

        # Write legacy anyscale project yaml
        with open(os.path.join(os.getcwd(), ".anyscale.yaml"), "wt") as f:
            f.write(f"project_id: {self.cluster_manager.project_id}")

    def upload(self, source: Optional[str] = None, target: Optional[str] = None):
        logger.info(
            f"Uploading {source or '<cwd>'} to {target or '<cwd>'} "
            f"using SessionController"
        )

        if source and os.path.isdir(source) and target:
            # Add trailing slashes
            source = os.path.join(source, "")
            target = os.path.join(target, "")

        self.session_controller.push(
            session_name=self.cluster_manager.cluster_name,
            source=source,
            target=target,
            config=None,
            all_nodes=False,
            no_warning=True,
        )

    def download(self, source: str, target: str):
        logger.info(
            f"Downloading {source or '<cwd>'} to {target or '<cwd>'} "
            f"using SessionController"
        )
        self.session_controller.pull(
            session_name=self.cluster_manager.cluster_name,
            source=source,
            target=target,
            config=None,
        )

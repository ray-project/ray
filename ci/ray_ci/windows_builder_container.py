import os

from ci.ray_ci.windows_container import WindowsContainer, WORKDIR


class WindowsBuilderContainer(WindowsContainer):
    def __init__(
        self,
        python_version: str,
        upload: bool,
    ) -> None:
        super().__init__(
            "windowsbuild",
            volumes=[
                f"{os.path.abspath(os.environ.get('RAYCI_CHECKOUT_DIR'))}:{WORKDIR}",
            ],
        )
        self.python_version = python_version
        self.upload = upload

    def run(self) -> None:
        cmds = [
            "powershell ci/pipeline/fix-windows-container-networking.ps1",
            f"export BUILD_ONE_PYTHON_ONLY={self.python_version}",
            "./python/build-wheel-windows.sh",
        ]
        if self.upload:
            cmds += ["./ci/build/copy_build_artifacts.sh"]
        self.run_script(cmds)

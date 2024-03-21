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
            "git config --global core.symlinks true",
            "git config --global core.autocrlf false",
            "git clone . ray",
            "cd ray",
            "git checkout -f 09abba26b5bf2707639bb637c208d062a47b46f6",
            f"export BUILD_ONE_PYTHON_ONLY={self.python_version}",
            "BUILDKITE_COMMIT=09abba26b5bf2707639bb637c208d062a47b46f6 ./python/build-wheel-windows.sh",
        ]
        if self.upload:
            cmds += ["./ci/build/copy_build_artifacts.sh"]
        self.run_script(cmds)

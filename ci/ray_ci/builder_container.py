import os

from ci.ray_ci.configs import BUILD_TYPES, PYTHON_VERSIONS
from ci.ray_ci.linux_container import LinuxContainer


class BuilderContainer(LinuxContainer):
    def __init__(
        self,
        python_version: str,
        build_type: str,
        architecture: str,
        upload: bool = False,
    ) -> None:
        super().__init__(
            "manylinux" if architecture == "x86_64" else f"manylinux-{architecture}",
            volumes=[f"{os.environ.get('RAYCI_CHECKOUT_DIR')}:/rayci"],
        )
        python_version_info = PYTHON_VERSIONS.get(python_version)
        assert build_type in BUILD_TYPES, f"build_type must be one of {BUILD_TYPES}"
        self.build_type = build_type
        self.bin_path = python_version_info["bin_path"]
        self.upload = upload

    def run(self) -> None:
        # chown is required to allow forge to upload the wheel
        cmds = []
        if self.build_type == "debug":
            cmds += ["export RAY_DEBUG_BUILD=debug"]

        if os.environ.get("RAYCI_DISABLE_CPP_WHEEL") == "true":
            cmds += ["export RAY_DISABLE_EXTRA_CPP=1"]
        if os.environ.get("RAYCI_DISABLE_JAVA", "") == "true":
            cmds += ["export RAY_INSTALL_JAVA=0"]

        cmds += [
            "./ci/build/build-manylinux-ray.sh",
            f"./ci/build/build-manylinux-wheel.sh {self.bin_path}",
            "chown -R 2000:100 /artifact-mount",
        ]

        if self.upload:
            cmds += ["./ci/build/copy_build_artifacts.sh wheel"]
        self.run_script(cmds)

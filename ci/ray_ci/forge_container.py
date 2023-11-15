import os

from ci.ray_ci.container import Container


class ForgeContainer(Container):
    def __init__(self, architecture: str) -> None:
        super().__init__(
            "forge" if architecture == "x86_64" else "forge-aarch64",
            volumes=[f"{os.environ.get('RAYCI_CHECKOUT_DIR')}:/rayci"],
        )

    def upload_wheel(self) -> None:
        self.run_script(["./ci/build/copy_build_artifacts.sh wheel"])

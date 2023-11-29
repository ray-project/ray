from ci.ray_ci.container import Container


class DocBuilderContainer(Container):
    def __init__(self) -> None:
        super().__init__("docbuild")
        self.install_ray(build_type="doc")

    def run(self) -> None:
        self.run_script(
            [
                "cd doc",
                "FAST=True make html",
            ]
        )

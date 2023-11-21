from ci.ray_ci.container import Container


class DocBuilderContainer(Container):
    def __init__(self) -> None:
        super().__init__("docbuild")
        self.install_ray(build_type="doc")

    def run(self) -> None:
        self.run_script(
            [
                "cd doc",
                "pip install -r requirements-doc.txt",
                "FAST=True make html",
            ]
        )

import pathlib
from typing import List


class Template:
    """Class which provides basic HTML templating."""

    def __init__(self, file: str):
        with open(pathlib.Path(__file__).parent / "templates" / file, "r") as f:
            self.template = f.read()

    def render(self, **kwargs) -> str:
        """Render a template by replacing instances of `{{ key }}` with `value`
        from the keyword arguments.

        Returns
        -------
        str
            HTML template with the keys of the kwargs replaced with corresponding
            values.
        """
        rendered = self.template
        for key, value in kwargs.items():
            rendered.replace("{{ " + key + " }}", value)
        return rendered

    @staticmethod
    def list_templates() -> List[pathlib.Path]:
        """List the available HTML templates.

        Returns
        -------
        List[pathlib.Path]
            A list of files with .html.j2 extensions inside ./templates/
        """
        return (pathlib.Path(__file__).parent / "templates").glob("*.html.j2")

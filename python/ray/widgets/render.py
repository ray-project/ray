from typing import List, Text, Union

from jinja2 import Environment, PackageLoader, Template, select_autoescape


def get_environment():
    return Environment(
        loader=PackageLoader("ray.widgets"),
        autoescape=select_autoescape(default=True, default_for_string=True),
    )


def get_template(template: Union[Template, Text]) -> Template:
    return get_environment().get_template(template)


def list_templates() -> List[str]:
    return get_environment().list_templates()

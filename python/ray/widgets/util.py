from typing import Any, Optional

from ray.util.annotations import DeveloperAPI
from ray.widgets import Template


@DeveloperAPI
def make_table_html_repr(
    obj: Any, title: Optional[str] = None, max_height: str = "none"
) -> str:
    """Generate a generic html repr using a table.

    Args:
        obj: Object for which a repr is to be generated
        title: If present, a title for the section is included
        max_height: Maximum height of the table; valid values
            are given by the max-height CSS property

    Returns:
        HTML representation of the object
    """
    try:
        from tabulate import tabulate
    except ImportError:
        return (
            "Tabulate isn't installed. Run "
            "`pip install tabulate` for rich notebook output."
        )

    data = {}
    for k, v in vars(obj).items():
        if isinstance(v, (str, bool, int, float)):
            data[k] = str(v)

        elif isinstance(v, dict) or hasattr(v, "__dict__"):
            data[k] = Template("scrollableTable.html.j2").render(
                table=tabulate(
                    v.items() if isinstance(v, dict) else vars(v).items(),
                    tablefmt="html",
                    showindex=False,
                    headers=["Setting", "Value"],
                ),
                max_height="none",
            )

    table = Template("scrollableTable.html.j2").render(
        table=tabulate(
            data.items(),
            tablefmt="unsafehtml",
            showindex=False,
            headers=["Setting", "Value"],
        ),
        max_height=max_height,
    )

    if title:
        content = Template("title_data.html.j2").render(title=title, data=table)
    else:
        content = table

    return content

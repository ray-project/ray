"""Command-line implementation of flake8."""
import sys
from typing import List, Optional

from flake8.main import application


def main(argv=None):
    # type: (Optional[List[str]]) -> None
    """Execute the main bit of the application.

    This handles the creation of an instance of :class:`Application`, runs it,
    and then exits the application.

    :param list argv:
        The arguments to be passed to the application for parsing.
    """
    if argv is None:
        argv = sys.argv[1:]

    app = application.Application()
    app.run(argv)
    app.exit()

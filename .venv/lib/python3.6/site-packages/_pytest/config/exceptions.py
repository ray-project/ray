class UsageError(Exception):
    """ error in pytest usage or invocation"""


class PrintHelp(Exception):
    """Raised when pytest should print it's help to skip the rest of the
    argument parsing and validation."""

    pass

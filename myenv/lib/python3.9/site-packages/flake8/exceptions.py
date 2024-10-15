"""Exception classes for all of Flake8."""
from typing import Dict


class Flake8Exception(Exception):
    """Plain Flake8 exception."""


class EarlyQuit(Flake8Exception):
    """Except raised when encountering a KeyboardInterrupt."""


class ExecutionError(Flake8Exception):
    """Exception raised during execution of Flake8."""


class FailedToLoadPlugin(Flake8Exception):
    """Exception raised when a plugin fails to load."""

    FORMAT = 'Flake8 failed to load plugin "%(name)s" due to %(exc)s.'

    def __init__(self, plugin_name, exception):
        # type: (str, Exception) -> None
        """Initialize our FailedToLoadPlugin exception."""
        self.plugin_name = plugin_name
        self.original_exception = exception
        super(FailedToLoadPlugin, self).__init__(plugin_name, exception)

    def __str__(self):  # type: () -> str
        """Format our exception message."""
        return self.FORMAT % {
            "name": self.plugin_name,
            "exc": self.original_exception,
        }


class InvalidSyntax(Flake8Exception):
    """Exception raised when tokenizing a file fails."""

    def __init__(self, exception):  # type: (Exception) -> None
        """Initialize our InvalidSyntax exception."""
        self.original_exception = exception
        self.error_message = "{0}: {1}".format(
            exception.__class__.__name__, exception.args[0]
        )
        self.error_code = "E902"
        self.line_number = 1
        self.column_number = 0
        super(InvalidSyntax, self).__init__(exception)

    def __str__(self):  # type: () -> str
        """Format our exception message."""
        return self.error_message


class PluginRequestedUnknownParameters(Flake8Exception):
    """The plugin requested unknown parameters."""

    FORMAT = '"%(name)s" requested unknown parameters causing %(exc)s'

    def __init__(self, plugin, exception):
        # type: (Dict[str, str], Exception) -> None
        """Pop certain keyword arguments for initialization."""
        self.plugin = plugin
        self.original_exception = exception
        super(PluginRequestedUnknownParameters, self).__init__(
            plugin, exception
        )

    def __str__(self):  # type: () -> str
        """Format our exception message."""
        return self.FORMAT % {
            "name": self.plugin["plugin_name"],
            "exc": self.original_exception,
        }


class PluginExecutionFailed(Flake8Exception):
    """The plugin failed during execution."""

    FORMAT = '"%(name)s" failed during execution due to "%(exc)s"'

    def __init__(self, plugin, exception):
        # type: (Dict[str, str], Exception) -> None
        """Utilize keyword arguments for message generation."""
        self.plugin = plugin
        self.original_exception = exception
        super(PluginExecutionFailed, self).__init__(plugin, exception)

    def __str__(self):  # type: () -> str
        """Format our exception message."""
        return self.FORMAT % {
            "name": self.plugin["plugin_name"],
            "exc": self.original_exception,
        }


class HookInstallationError(Flake8Exception):
    """Parent exception for all hooks errors."""


class GitHookAlreadyExists(HookInstallationError):
    """Exception raised when the git pre-commit hook file already exists."""

    def __init__(self, path):  # type: (str) -> None
        """Initialize the exception message from the `path`."""
        self.path = path
        tmpl = (
            "The Git pre-commit hook ({0}) already exists. To convince "
            "Flake8 to install the hook, please remove the existing "
            "hook."
        )
        super(GitHookAlreadyExists, self).__init__(tmpl.format(self.path))


class MercurialHookAlreadyExists(HookInstallationError):
    """Exception raised when a mercurial hook is already configured."""

    hook_name = None  # type: str

    def __init__(self, path, value):  # type: (str, str) -> None
        """Initialize the relevant attributes."""
        self.path = path
        self.value = value
        tmpl = (
            'The Mercurial {0} hook already exists with "{1}" in {2}. '
            "To convince Flake8 to install the hook, please remove the "
            "{0} configuration from the [hooks] section of your hgrc."
        )
        msg = tmpl.format(self.hook_name, self.value, self.path)
        super(MercurialHookAlreadyExists, self).__init__(msg)


class MercurialCommitHookAlreadyExists(MercurialHookAlreadyExists):
    """Exception raised when the hg commit hook is already configured."""

    hook_name = "commit"


class MercurialQRefreshHookAlreadyExists(MercurialHookAlreadyExists):
    """Exception raised when the hg commit hook is already configured."""

    hook_name = "qrefresh"

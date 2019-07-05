"""
This module contains deprecation messages and bits of code used elsewhere in the codebase
that is planned to be removed in the next pytest release.

Keeping it in a central location makes it easy to track what is deprecated and should
be removed when the time comes.

All constants defined in this module should be either PytestWarning instances or UnformattedWarning
in case of warnings which need to format their messages.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from _pytest.warning_types import PytestDeprecationWarning
from _pytest.warning_types import RemovedInPytest4Warning
from _pytest.warning_types import UnformattedWarning

YIELD_TESTS = "yield tests were removed in pytest 4.0 - {name} will be ignored"


FIXTURE_FUNCTION_CALL = (
    'Fixture "{name}" called directly. Fixtures are not meant to be called directly,\n'
    "but are created automatically when test functions request them as parameters.\n"
    "See https://docs.pytest.org/en/latest/fixture.html for more information about fixtures, and\n"
    "https://docs.pytest.org/en/latest/deprecations.html#calling-fixtures-directly about how to update your code."
)

FIXTURE_NAMED_REQUEST = PytestDeprecationWarning(
    "'request' is a reserved name for fixtures and will raise an error in future versions"
)

CFG_PYTEST_SECTION = "[pytest] section in {filename} files is no longer supported, change to [tool:pytest] instead."

GETFUNCARGVALUE = RemovedInPytest4Warning(
    "getfuncargvalue is deprecated, use getfixturevalue"
)

RAISES_MESSAGE_PARAMETER = PytestDeprecationWarning(
    "The 'message' parameter is deprecated.\n"
    "(did you mean to use `match='some regex'` to check the exception message?)\n"
    "Please comment on https://github.com/pytest-dev/pytest/issues/3974 "
    "if you have concerns about removal of this parameter."
)

RESULT_LOG = PytestDeprecationWarning(
    "--result-log is deprecated and scheduled for removal in pytest 5.0.\n"
    "See https://docs.pytest.org/en/latest/deprecations.html#result-log-result-log for more information."
)

RAISES_EXEC = PytestDeprecationWarning(
    "raises(..., 'code(as_a_string)') is deprecated, use the context manager form or use `exec()` directly\n\n"
    "See https://docs.pytest.org/en/latest/deprecations.html#raises-warns-exec"
)
WARNS_EXEC = PytestDeprecationWarning(
    "warns(..., 'code(as_a_string)') is deprecated, use the context manager form or use `exec()` directly.\n\n"
    "See https://docs.pytest.org/en/latest/deprecations.html#raises-warns-exec"
)

PYTEST_PLUGINS_FROM_NON_TOP_LEVEL_CONFTEST = (
    "Defining 'pytest_plugins' in a non-top-level conftest is no longer supported "
    "because it affects the entire directory tree in a non-explicit way.\n"
    "  {}\n"
    "Please move it to a top level conftest file at the rootdir:\n"
    "  {}\n"
    "For more information, visit:\n"
    "  https://docs.pytest.org/en/latest/deprecations.html#pytest-plugins-in-non-top-level-conftest-files"
)

PYTEST_CONFIG_GLOBAL = PytestDeprecationWarning(
    "the `pytest.config` global is deprecated.  Please use `request.config` "
    "or `pytest_configure` (if you're a pytest plugin) instead."
)

PYTEST_ENSURETEMP = RemovedInPytest4Warning(
    "pytest/tmpdir_factory.ensuretemp is deprecated, \n"
    "please use the tmp_path fixture or tmp_path_factory.mktemp"
)

PYTEST_LOGWARNING = PytestDeprecationWarning(
    "pytest_logwarning is deprecated, no longer being called, and will be removed soon\n"
    "please use pytest_warning_captured instead"
)

PYTEST_WARNS_UNKNOWN_KWARGS = UnformattedWarning(
    PytestDeprecationWarning,
    "pytest.warns() got unexpected keyword arguments: {args!r}.\n"
    "This will be an error in future versions.",
)

PYTEST_PARAM_UNKNOWN_KWARGS = UnformattedWarning(
    PytestDeprecationWarning,
    "pytest.param() got unexpected keyword arguments: {args!r}.\n"
    "This will be an error in future versions.",
)

""" hook specifications for pytest plugins, invoked from main.py and builtin plugins.  """
from pluggy import HookspecMarker

from _pytest.deprecated import PYTEST_LOGWARNING

hookspec = HookspecMarker("pytest")

# -------------------------------------------------------------------------
# Initialization hooks called for every plugin
# -------------------------------------------------------------------------


@hookspec(historic=True)
def pytest_addhooks(pluginmanager):
    """called at plugin registration time to allow adding new hooks via a call to
    ``pluginmanager.add_hookspecs(module_or_class, prefix)``.


    :param _pytest.config.PytestPluginManager pluginmanager: pytest plugin manager

    .. note::
        This hook is incompatible with ``hookwrapper=True``.
    """


@hookspec(historic=True)
def pytest_plugin_registered(plugin, manager):
    """ a new pytest plugin got registered.

    :param plugin: the plugin module or instance
    :param _pytest.config.PytestPluginManager manager: pytest plugin manager

    .. note::
        This hook is incompatible with ``hookwrapper=True``.
    """


@hookspec(historic=True)
def pytest_addoption(parser):
    """register argparse-style options and ini-style config values,
    called once at the beginning of a test run.

    .. note::

        This function should be implemented only in plugins or ``conftest.py``
        files situated at the tests root directory due to how pytest
        :ref:`discovers plugins during startup <pluginorder>`.

    :arg _pytest.config.Parser parser: To add command line options, call
        :py:func:`parser.addoption(...) <_pytest.config.Parser.addoption>`.
        To add ini-file values call :py:func:`parser.addini(...)
        <_pytest.config.Parser.addini>`.

    Options can later be accessed through the
    :py:class:`config <_pytest.config.Config>` object, respectively:

    - :py:func:`config.getoption(name) <_pytest.config.Config.getoption>` to
      retrieve the value of a command line option.

    - :py:func:`config.getini(name) <_pytest.config.Config.getini>` to retrieve
      a value read from an ini-style file.

    The config object is passed around on many internal objects via the ``.config``
    attribute or can be retrieved as the ``pytestconfig`` fixture.

    .. note::
        This hook is incompatible with ``hookwrapper=True``.
    """


@hookspec(historic=True)
def pytest_configure(config):
    """
    Allows plugins and conftest files to perform initial configuration.

    This hook is called for every plugin and initial conftest file
    after command line options have been parsed.

    After that, the hook is called for other conftest files as they are
    imported.

    .. note::
        This hook is incompatible with ``hookwrapper=True``.

    :arg _pytest.config.Config config: pytest config object
    """


# -------------------------------------------------------------------------
# Bootstrapping hooks called for plugins registered early enough:
# internal and 3rd party plugins.
# -------------------------------------------------------------------------


@hookspec(firstresult=True)
def pytest_cmdline_parse(pluginmanager, args):
    """return initialized config object, parsing the specified args.

    Stops at first non-None result, see :ref:`firstresult`

    .. note::
        This hook will only be called for plugin classes passed to the ``plugins`` arg when using `pytest.main`_ to
        perform an in-process test run.

    :param _pytest.config.PytestPluginManager pluginmanager: pytest plugin manager
    :param list[str] args: list of arguments passed on the command line
    """


def pytest_cmdline_preparse(config, args):
    """(**Deprecated**) modify command line arguments before option parsing.

    This hook is considered deprecated and will be removed in a future pytest version. Consider
    using :func:`pytest_load_initial_conftests` instead.

    .. note::
        This hook will not be called for ``conftest.py`` files, only for setuptools plugins.

    :param _pytest.config.Config config: pytest config object
    :param list[str] args: list of arguments passed on the command line
    """


@hookspec(firstresult=True)
def pytest_cmdline_main(config):
    """ called for performing the main command line action. The default
    implementation will invoke the configure hooks and runtest_mainloop.

    .. note::
        This hook will not be called for ``conftest.py`` files, only for setuptools plugins.

    Stops at first non-None result, see :ref:`firstresult`

    :param _pytest.config.Config config: pytest config object
    """


def pytest_load_initial_conftests(early_config, parser, args):
    """ implements the loading of initial conftest files ahead
    of command line option parsing.

    .. note::
        This hook will not be called for ``conftest.py`` files, only for setuptools plugins.

    :param _pytest.config.Config early_config: pytest config object
    :param list[str] args: list of arguments passed on the command line
    :param _pytest.config.Parser parser: to add command line options
    """


# -------------------------------------------------------------------------
# collection hooks
# -------------------------------------------------------------------------


@hookspec(firstresult=True)
def pytest_collection(session):
    """Perform the collection protocol for the given session.

    Stops at first non-None result, see :ref:`firstresult`.

    :param _pytest.main.Session session: the pytest session object
    """


def pytest_collection_modifyitems(session, config, items):
    """ called after collection has been performed, may filter or re-order
    the items in-place.

    :param _pytest.main.Session session: the pytest session object
    :param _pytest.config.Config config: pytest config object
    :param List[_pytest.nodes.Item] items: list of item objects
    """


def pytest_collection_finish(session):
    """ called after collection has been performed and modified.

    :param _pytest.main.Session session: the pytest session object
    """


@hookspec(firstresult=True)
def pytest_ignore_collect(path, config):
    """ return True to prevent considering this path for collection.
    This hook is consulted for all files and directories prior to calling
    more specific hooks.

    Stops at first non-None result, see :ref:`firstresult`

    :param path: a :py:class:`py.path.local` - the path to analyze
    :param _pytest.config.Config config: pytest config object
    """


@hookspec(firstresult=True)
def pytest_collect_directory(path, parent):
    """ called before traversing a directory for collection files.

    Stops at first non-None result, see :ref:`firstresult`

    :param path: a :py:class:`py.path.local` - the path to analyze
    """


def pytest_collect_file(path, parent):
    """ return collection Node or None for the given path. Any new node
    needs to have the specified ``parent`` as a parent.

    :param path: a :py:class:`py.path.local` - the path to collect
    """


# logging hooks for collection


def pytest_collectstart(collector):
    """ collector starts collecting. """


def pytest_itemcollected(item):
    """ we just collected a test item. """


def pytest_collectreport(report):
    """ collector finished collecting. """


def pytest_deselected(items):
    """ called for test items deselected, e.g. by keyword. """


@hookspec(firstresult=True)
def pytest_make_collect_report(collector):
    """ perform ``collector.collect()`` and return a CollectReport.

    Stops at first non-None result, see :ref:`firstresult` """


# -------------------------------------------------------------------------
# Python test function related hooks
# -------------------------------------------------------------------------


@hookspec(firstresult=True)
def pytest_pycollect_makemodule(path, parent):
    """ return a Module collector or None for the given path.
    This hook will be called for each matching test module path.
    The pytest_collect_file hook needs to be used if you want to
    create test modules for files that do not match as a test module.

    Stops at first non-None result, see :ref:`firstresult`

    :param path: a :py:class:`py.path.local` - the path of module to collect
    """


@hookspec(firstresult=True)
def pytest_pycollect_makeitem(collector, name, obj):
    """ return custom item/collector for a python object in a module, or None.

    Stops at first non-None result, see :ref:`firstresult` """


@hookspec(firstresult=True)
def pytest_pyfunc_call(pyfuncitem):
    """ call underlying test function.

    Stops at first non-None result, see :ref:`firstresult` """


def pytest_generate_tests(metafunc):
    """ generate (multiple) parametrized calls to a test function."""


@hookspec(firstresult=True)
def pytest_make_parametrize_id(config, val, argname):
    """Return a user-friendly string representation of the given ``val`` that will be used
    by @pytest.mark.parametrize calls. Return None if the hook doesn't know about ``val``.
    The parameter name is available as ``argname``, if required.

    Stops at first non-None result, see :ref:`firstresult`

    :param _pytest.config.Config config: pytest config object
    :param val: the parametrized value
    :param str argname: the automatic parameter name produced by pytest
    """


# -------------------------------------------------------------------------
# generic runtest related hooks
# -------------------------------------------------------------------------


@hookspec(firstresult=True)
def pytest_runtestloop(session):
    """ called for performing the main runtest loop
    (after collection finished).

    Stops at first non-None result, see :ref:`firstresult`

    :param _pytest.main.Session session: the pytest session object
    """


def pytest_itemstart(item, node):
    """(**Deprecated**) use pytest_runtest_logstart. """


@hookspec(firstresult=True)
def pytest_runtest_protocol(item, nextitem):
    """ implements the runtest_setup/call/teardown protocol for
    the given test item, including capturing exceptions and calling
    reporting hooks.

    :arg item: test item for which the runtest protocol is performed.

    :arg nextitem: the scheduled-to-be-next test item (or None if this
                   is the end my friend).  This argument is passed on to
                   :py:func:`pytest_runtest_teardown`.

    :return boolean: True if no further hook implementations should be invoked.


    Stops at first non-None result, see :ref:`firstresult` """


def pytest_runtest_logstart(nodeid, location):
    """ signal the start of running a single test item.

    This hook will be called **before** :func:`pytest_runtest_setup`, :func:`pytest_runtest_call` and
    :func:`pytest_runtest_teardown` hooks.

    :param str nodeid: full id of the item
    :param location: a triple of ``(filename, linenum, testname)``
    """


def pytest_runtest_logfinish(nodeid, location):
    """ signal the complete finish of running a single test item.

    This hook will be called **after** :func:`pytest_runtest_setup`, :func:`pytest_runtest_call` and
    :func:`pytest_runtest_teardown` hooks.

    :param str nodeid: full id of the item
    :param location: a triple of ``(filename, linenum, testname)``
    """


def pytest_runtest_setup(item):
    """ called before ``pytest_runtest_call(item)``. """


def pytest_runtest_call(item):
    """ called to execute the test ``item``. """


def pytest_runtest_teardown(item, nextitem):
    """ called after ``pytest_runtest_call``.

    :arg nextitem: the scheduled-to-be-next test item (None if no further
                   test item is scheduled).  This argument can be used to
                   perform exact teardowns, i.e. calling just enough finalizers
                   so that nextitem only needs to call setup-functions.
    """


@hookspec(firstresult=True)
def pytest_runtest_makereport(item, call):
    """ return a :py:class:`_pytest.runner.TestReport` object
    for the given :py:class:`pytest.Item <_pytest.main.Item>` and
    :py:class:`_pytest.runner.CallInfo`.

    Stops at first non-None result, see :ref:`firstresult` """


def pytest_runtest_logreport(report):
    """ process a test setup/call/teardown report relating to
    the respective phase of executing a test. """


@hookspec(firstresult=True)
def pytest_report_to_serializable(config, report):
    """
    .. warning::
        This hook is experimental and subject to change between pytest releases, even
        bug fixes.

        The intent is for this to be used by plugins maintained by the core-devs, such
        as ``pytest-xdist``, ``pytest-subtests``, and as a replacement for the internal
        'resultlog' plugin.

        In the future it might become part of the public hook API.

    Serializes the given report object into a data structure suitable for sending
    over the wire, e.g. converted to JSON.
    """


@hookspec(firstresult=True)
def pytest_report_from_serializable(config, data):
    """
    .. warning::
        This hook is experimental and subject to change between pytest releases, even
        bug fixes.

        The intent is for this to be used by plugins maintained by the core-devs, such
        as ``pytest-xdist``, ``pytest-subtests``, and as a replacement for the internal
        'resultlog' plugin.

        In the future it might become part of the public hook API.

    Restores a report object previously serialized with pytest_report_to_serializable().
    """


# -------------------------------------------------------------------------
# Fixture related hooks
# -------------------------------------------------------------------------


@hookspec(firstresult=True)
def pytest_fixture_setup(fixturedef, request):
    """ performs fixture setup execution.

    :return: The return value of the call to the fixture function

    Stops at first non-None result, see :ref:`firstresult`

    .. note::
        If the fixture function returns None, other implementations of
        this hook function will continue to be called, according to the
        behavior of the :ref:`firstresult` option.
    """


def pytest_fixture_post_finalizer(fixturedef, request):
    """ called after fixture teardown, but before the cache is cleared so
    the fixture result cache ``fixturedef.cached_result`` can
    still be accessed."""


# -------------------------------------------------------------------------
# test session related hooks
# -------------------------------------------------------------------------


def pytest_sessionstart(session):
    """ called after the ``Session`` object has been created and before performing collection
    and entering the run test loop.

    :param _pytest.main.Session session: the pytest session object
    """


def pytest_sessionfinish(session, exitstatus):
    """ called after whole test run finished, right before returning the exit status to the system.

    :param _pytest.main.Session session: the pytest session object
    :param int exitstatus: the status which pytest will return to the system
    """


def pytest_unconfigure(config):
    """ called before test process is exited.

    :param _pytest.config.Config config: pytest config object
    """


# -------------------------------------------------------------------------
# hooks for customizing the assert methods
# -------------------------------------------------------------------------


def pytest_assertrepr_compare(config, op, left, right):
    """return explanation for comparisons in failing assert expressions.

    Return None for no custom explanation, otherwise return a list
    of strings.  The strings will be joined by newlines but any newlines
    *in* a string will be escaped.  Note that all but the first line will
    be indented slightly, the intention is for the first line to be a summary.

    :param _pytest.config.Config config: pytest config object
    """


# -------------------------------------------------------------------------
# hooks for influencing reporting (invoked from _pytest_terminal)
# -------------------------------------------------------------------------


def pytest_report_header(config, startdir):
    """ return a string or list of strings to be displayed as header info for terminal reporting.

    :param _pytest.config.Config config: pytest config object
    :param startdir: py.path object with the starting dir

    .. note::

        This function should be implemented only in plugins or ``conftest.py``
        files situated at the tests root directory due to how pytest
        :ref:`discovers plugins during startup <pluginorder>`.
    """


def pytest_report_collectionfinish(config, startdir, items):
    """
    .. versionadded:: 3.2

    return a string or list of strings to be displayed after collection has finished successfully.

    This strings will be displayed after the standard "collected X items" message.

    :param _pytest.config.Config config: pytest config object
    :param startdir: py.path object with the starting dir
    :param items: list of pytest items that are going to be executed; this list should not be modified.
    """


@hookspec(firstresult=True)
def pytest_report_teststatus(report, config):
    """ return result-category, shortletter and verbose word for reporting.

    :param _pytest.config.Config config: pytest config object

    Stops at first non-None result, see :ref:`firstresult` """


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Add a section to terminal summary reporting.

    :param _pytest.terminal.TerminalReporter terminalreporter: the internal terminal reporter object
    :param int exitstatus: the exit status that will be reported back to the OS
    :param _pytest.config.Config config: pytest config object

    .. versionadded:: 4.2
        The ``config`` parameter.
    """


@hookspec(historic=True, warn_on_impl=PYTEST_LOGWARNING)
def pytest_logwarning(message, code, nodeid, fslocation):
    """
    .. deprecated:: 3.8

        This hook is will stop working in a future release.

        pytest no longer triggers this hook, but the
        terminal writer still implements it to display warnings issued by
        :meth:`_pytest.config.Config.warn` and :meth:`_pytest.nodes.Node.warn`. Calling those functions will be
        an error in future releases.

    process a warning specified by a message, a code string,
    a nodeid and fslocation (both of which may be None
    if the warning is not tied to a particular node/location).

    .. note::
        This hook is incompatible with ``hookwrapper=True``.
    """


@hookspec(historic=True)
def pytest_warning_captured(warning_message, when, item):
    """
    Process a warning captured by the internal pytest warnings plugin.

    :param warnings.WarningMessage warning_message:
        The captured warning. This is the same object produced by :py:func:`warnings.catch_warnings`, and contains
        the same attributes as the parameters of :py:func:`warnings.showwarning`.

    :param str when:
        Indicates when the warning was captured. Possible values:

        * ``"config"``: during pytest configuration/initialization stage.
        * ``"collect"``: during test collection.
        * ``"runtest"``: during test execution.

    :param pytest.Item|None item:
        **DEPRECATED**: This parameter is incompatible with ``pytest-xdist``, and will always receive ``None``
        in a future release.

        The item being executed if ``when`` is ``"runtest"``, otherwise ``None``.
    """


# -------------------------------------------------------------------------
# doctest hooks
# -------------------------------------------------------------------------


@hookspec(firstresult=True)
def pytest_doctest_prepare_content(content):
    """ return processed content for a given doctest

    Stops at first non-None result, see :ref:`firstresult` """


# -------------------------------------------------------------------------
# error handling and internal debugging hooks
# -------------------------------------------------------------------------


def pytest_internalerror(excrepr, excinfo):
    """ called for internal errors. """


def pytest_keyboard_interrupt(excinfo):
    """ called for keyboard interrupt. """


def pytest_exception_interact(node, call, report):
    """called when an exception was raised which can potentially be
    interactively handled.

    This hook is only called if an exception was raised
    that is not an internal exception like ``skip.Exception``.
    """


def pytest_enter_pdb(config, pdb):
    """ called upon pdb.set_trace(), can be used by plugins to take special
    action just before the python debugger enters in interactive mode.

    :param _pytest.config.Config config: pytest config object
    :param pdb.Pdb pdb: Pdb instance
    """


def pytest_leave_pdb(config, pdb):
    """ called when leaving pdb (e.g. with continue after pdb.set_trace()).

    Can be used by plugins to take special action just after the python
    debugger leaves interactive mode.

    :param _pytest.config.Config config: pytest config object
    :param pdb.Pdb pdb: Pdb instance
    """

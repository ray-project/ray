""" interactive debugging with PDB, the Python Debugger. """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import pdb
import sys
from doctest import UnexpectedException

from _pytest import outcomes
from _pytest.config import hookimpl
from _pytest.config.exceptions import UsageError


def _validate_usepdb_cls(value):
    """Validate syntax of --pdbcls option."""
    try:
        modname, classname = value.split(":")
    except ValueError:
        raise argparse.ArgumentTypeError(
            "{!r} is not in the format 'modname:classname'".format(value)
        )
    return (modname, classname)


def pytest_addoption(parser):
    group = parser.getgroup("general")
    group._addoption(
        "--pdb",
        dest="usepdb",
        action="store_true",
        help="start the interactive Python debugger on errors or KeyboardInterrupt.",
    )
    group._addoption(
        "--pdbcls",
        dest="usepdb_cls",
        metavar="modulename:classname",
        type=_validate_usepdb_cls,
        help="start a custom interactive Python debugger on errors. "
        "For example: --pdbcls=IPython.terminal.debugger:TerminalPdb",
    )
    group._addoption(
        "--trace",
        dest="trace",
        action="store_true",
        help="Immediately break when running each test.",
    )


def _import_pdbcls(modname, classname):
    try:
        __import__(modname)
        mod = sys.modules[modname]

        # Handle --pdbcls=pdb:pdb.Pdb (useful e.g. with pdbpp).
        parts = classname.split(".")
        pdb_cls = getattr(mod, parts[0])
        for part in parts[1:]:
            pdb_cls = getattr(pdb_cls, part)

        return pdb_cls
    except Exception as exc:
        value = ":".join((modname, classname))
        raise UsageError("--pdbcls: could not import {!r}: {}".format(value, exc))


def pytest_configure(config):
    pdb_cls = config.getvalue("usepdb_cls")
    if pdb_cls:
        pdb_cls = _import_pdbcls(*pdb_cls)
    else:
        pdb_cls = pdb.Pdb

    if config.getvalue("trace"):
        config.pluginmanager.register(PdbTrace(), "pdbtrace")
    if config.getvalue("usepdb"):
        config.pluginmanager.register(PdbInvoke(), "pdbinvoke")

    pytestPDB._saved.append(
        (pdb.set_trace, pytestPDB._pluginmanager, pytestPDB._config, pytestPDB._pdb_cls)
    )
    pdb.set_trace = pytestPDB.set_trace
    pytestPDB._pluginmanager = config.pluginmanager
    pytestPDB._config = config
    pytestPDB._pdb_cls = pdb_cls

    # NOTE: not using pytest_unconfigure, since it might get called although
    #       pytest_configure was not (if another plugin raises UsageError).
    def fin():
        (
            pdb.set_trace,
            pytestPDB._pluginmanager,
            pytestPDB._config,
            pytestPDB._pdb_cls,
        ) = pytestPDB._saved.pop()

    config._cleanup.append(fin)


class pytestPDB(object):
    """ Pseudo PDB that defers to the real pdb. """

    _pluginmanager = None
    _config = None
    _pdb_cls = pdb.Pdb
    _saved = []
    _recursive_debug = 0

    @classmethod
    def _is_capturing(cls, capman):
        if capman:
            return capman.is_capturing()
        return False

    @classmethod
    def _init_pdb(cls, *args, **kwargs):
        """ Initialize PDB debugging, dropping any IO capturing. """
        import _pytest.config

        if cls._pluginmanager is not None:
            capman = cls._pluginmanager.getplugin("capturemanager")
            if capman:
                capman.suspend(in_=True)
            tw = _pytest.config.create_terminal_writer(cls._config)
            tw.line()
            if cls._recursive_debug == 0:
                # Handle header similar to pdb.set_trace in py37+.
                header = kwargs.pop("header", None)
                if header is not None:
                    tw.sep(">", header)
                else:
                    capturing = cls._is_capturing(capman)
                    if capturing:
                        if capturing == "global":
                            tw.sep(">", "PDB set_trace (IO-capturing turned off)")
                        else:
                            tw.sep(
                                ">",
                                "PDB set_trace (IO-capturing turned off for %s)"
                                % capturing,
                            )
                    else:
                        tw.sep(">", "PDB set_trace")

            class PytestPdbWrapper(cls._pdb_cls, object):
                _pytest_capman = capman
                _continued = False

                def do_debug(self, arg):
                    cls._recursive_debug += 1
                    ret = super(PytestPdbWrapper, self).do_debug(arg)
                    cls._recursive_debug -= 1
                    return ret

                def do_continue(self, arg):
                    ret = super(PytestPdbWrapper, self).do_continue(arg)
                    if cls._recursive_debug == 0:
                        tw = _pytest.config.create_terminal_writer(cls._config)
                        tw.line()

                        capman = self._pytest_capman
                        capturing = pytestPDB._is_capturing(capman)
                        if capturing:
                            if capturing == "global":
                                tw.sep(">", "PDB continue (IO-capturing resumed)")
                            else:
                                tw.sep(
                                    ">",
                                    "PDB continue (IO-capturing resumed for %s)"
                                    % capturing,
                                )
                            capman.resume()
                        else:
                            tw.sep(">", "PDB continue")
                    cls._pluginmanager.hook.pytest_leave_pdb(
                        config=cls._config, pdb=self
                    )
                    self._continued = True
                    return ret

                do_c = do_cont = do_continue

                def do_quit(self, arg):
                    """Raise Exit outcome when quit command is used in pdb.

                    This is a bit of a hack - it would be better if BdbQuit
                    could be handled, but this would require to wrap the
                    whole pytest run, and adjust the report etc.
                    """
                    ret = super(PytestPdbWrapper, self).do_quit(arg)

                    if cls._recursive_debug == 0:
                        outcomes.exit("Quitting debugger")

                    return ret

                do_q = do_quit
                do_exit = do_quit

                def setup(self, f, tb):
                    """Suspend on setup().

                    Needed after do_continue resumed, and entering another
                    breakpoint again.
                    """
                    ret = super(PytestPdbWrapper, self).setup(f, tb)
                    if not ret and self._continued:
                        # pdb.setup() returns True if the command wants to exit
                        # from the interaction: do not suspend capturing then.
                        if self._pytest_capman:
                            self._pytest_capman.suspend_global_capture(in_=True)
                    return ret

            _pdb = PytestPdbWrapper(**kwargs)
            cls._pluginmanager.hook.pytest_enter_pdb(config=cls._config, pdb=_pdb)
        else:
            _pdb = cls._pdb_cls(**kwargs)
        return _pdb

    @classmethod
    def set_trace(cls, *args, **kwargs):
        """Invoke debugging via ``Pdb.set_trace``, dropping any IO capturing."""
        frame = sys._getframe().f_back
        _pdb = cls._init_pdb(*args, **kwargs)
        _pdb.set_trace(frame)


class PdbInvoke(object):
    def pytest_exception_interact(self, node, call, report):
        capman = node.config.pluginmanager.getplugin("capturemanager")
        if capman:
            capman.suspend_global_capture(in_=True)
            out, err = capman.read_global_capture()
            sys.stdout.write(out)
            sys.stdout.write(err)
        _enter_pdb(node, call.excinfo, report)

    def pytest_internalerror(self, excrepr, excinfo):
        tb = _postmortem_traceback(excinfo)
        post_mortem(tb)


class PdbTrace(object):
    @hookimpl(hookwrapper=True)
    def pytest_pyfunc_call(self, pyfuncitem):
        _test_pytest_function(pyfuncitem)
        yield


def _test_pytest_function(pyfuncitem):
    _pdb = pytestPDB._init_pdb()
    testfunction = pyfuncitem.obj
    pyfuncitem.obj = _pdb.runcall
    if "func" in pyfuncitem._fixtureinfo.argnames:  # pragma: no branch
        raise ValueError("--trace can't be used with a fixture named func!")
    pyfuncitem.funcargs["func"] = testfunction
    new_list = list(pyfuncitem._fixtureinfo.argnames)
    new_list.append("func")
    pyfuncitem._fixtureinfo.argnames = tuple(new_list)


def _enter_pdb(node, excinfo, rep):
    # XXX we re-use the TerminalReporter's terminalwriter
    # because this seems to avoid some encoding related troubles
    # for not completely clear reasons.
    tw = node.config.pluginmanager.getplugin("terminalreporter")._tw
    tw.line()

    showcapture = node.config.option.showcapture

    for sectionname, content in (
        ("stdout", rep.capstdout),
        ("stderr", rep.capstderr),
        ("log", rep.caplog),
    ):
        if showcapture in (sectionname, "all") and content:
            tw.sep(">", "captured " + sectionname)
            if content[-1:] == "\n":
                content = content[:-1]
            tw.line(content)

    tw.sep(">", "traceback")
    rep.toterminal(tw)
    tw.sep(">", "entering PDB")
    tb = _postmortem_traceback(excinfo)
    rep._pdbshown = True
    post_mortem(tb)
    return rep


def _postmortem_traceback(excinfo):
    if isinstance(excinfo.value, UnexpectedException):
        # A doctest.UnexpectedException is not useful for post_mortem.
        # Use the underlying exception instead:
        return excinfo.value.exc_info[2]
    else:
        return excinfo._excinfo[2]


def _find_last_non_hidden_frame(stack):
    i = max(0, len(stack) - 1)
    while i and stack[i][0].f_locals.get("__tracebackhide__", False):
        i -= 1
    return i


def post_mortem(t):
    class Pdb(pytestPDB._pdb_cls, object):
        def get_stack(self, f, t):
            stack, i = super(Pdb, self).get_stack(f, t)
            if f is None:
                i = _find_last_non_hidden_frame(stack)
            return stack, i

    p = Pdb()
    p.reset()
    p.interaction(None, t)
    if p.quitting:
        outcomes.exit("Quitting debugger")

"""
per-test stdout/stderr capturing mechanism.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import contextlib
import io
import os
import sys
from io import UnsupportedOperation
from tempfile import TemporaryFile

import six

import pytest
from _pytest.compat import _PY3
from _pytest.compat import CaptureIO

patchsysdict = {0: "stdin", 1: "stdout", 2: "stderr"}


def pytest_addoption(parser):
    group = parser.getgroup("general")
    group._addoption(
        "--capture",
        action="store",
        default="fd" if hasattr(os, "dup") else "sys",
        metavar="method",
        choices=["fd", "sys", "no"],
        help="per-test capturing method: one of fd|sys|no.",
    )
    group._addoption(
        "-s",
        action="store_const",
        const="no",
        dest="capture",
        help="shortcut for --capture=no.",
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_load_initial_conftests(early_config, parser, args):
    ns = early_config.known_args_namespace
    if ns.capture == "fd":
        _py36_windowsconsoleio_workaround(sys.stdout)
    _colorama_workaround()
    _readline_workaround()
    pluginmanager = early_config.pluginmanager
    capman = CaptureManager(ns.capture)
    pluginmanager.register(capman, "capturemanager")

    # make sure that capturemanager is properly reset at final shutdown
    early_config.add_cleanup(capman.stop_global_capturing)

    # finally trigger conftest loading but while capturing (issue93)
    capman.start_global_capturing()
    outcome = yield
    capman.suspend_global_capture()
    if outcome.excinfo is not None:
        out, err = capman.read_global_capture()
        sys.stdout.write(out)
        sys.stderr.write(err)


class CaptureManager(object):
    """
    Capture plugin, manages that the appropriate capture method is enabled/disabled during collection and each
    test phase (setup, call, teardown). After each of those points, the captured output is obtained and
    attached to the collection/runtest report.

    There are two levels of capture:
    * global: which is enabled by default and can be suppressed by the ``-s`` option. This is always enabled/disabled
      during collection and each test phase.
    * fixture: when a test function or one of its fixture depend on the ``capsys`` or ``capfd`` fixtures. In this
      case special handling is needed to ensure the fixtures take precedence over the global capture.
    """

    def __init__(self, method):
        self._method = method
        self._global_capturing = None
        self._current_item = None

    def __repr__(self):
        return "<CaptureManager _method=%r _global_capturing=%r _current_item=%r>" % (
            self._method,
            self._global_capturing,
            self._current_item,
        )

    def _getcapture(self, method):
        if method == "fd":
            return MultiCapture(out=True, err=True, Capture=FDCapture)
        elif method == "sys":
            return MultiCapture(out=True, err=True, Capture=SysCapture)
        elif method == "no":
            return MultiCapture(out=False, err=False, in_=False)
        raise ValueError("unknown capturing method: %r" % method)  # pragma: no cover

    def is_capturing(self):
        if self.is_globally_capturing():
            return "global"
        capture_fixture = getattr(self._current_item, "_capture_fixture", None)
        if capture_fixture is not None:
            return (
                "fixture %s" % self._current_item._capture_fixture.request.fixturename
            )
        return False

    # Global capturing control

    def is_globally_capturing(self):
        return self._method != "no"

    def start_global_capturing(self):
        assert self._global_capturing is None
        self._global_capturing = self._getcapture(self._method)
        self._global_capturing.start_capturing()

    def stop_global_capturing(self):
        if self._global_capturing is not None:
            self._global_capturing.pop_outerr_to_orig()
            self._global_capturing.stop_capturing()
            self._global_capturing = None

    def resume_global_capture(self):
        # During teardown of the python process, and on rare occasions, capture
        # attributes can be `None` while trying to resume global capture.
        if self._global_capturing is not None:
            self._global_capturing.resume_capturing()

    def suspend_global_capture(self, in_=False):
        cap = getattr(self, "_global_capturing", None)
        if cap is not None:
            cap.suspend_capturing(in_=in_)

    def suspend(self, in_=False):
        # Need to undo local capsys-et-al if it exists before disabling global capture.
        self.suspend_fixture(self._current_item)
        self.suspend_global_capture(in_)

    def resume(self):
        self.resume_global_capture()
        self.resume_fixture(self._current_item)

    def read_global_capture(self):
        return self._global_capturing.readouterr()

    # Fixture Control (it's just forwarding, think about removing this later)

    def activate_fixture(self, item):
        """If the current item is using ``capsys`` or ``capfd``, activate them so they take precedence over
        the global capture.
        """
        fixture = getattr(item, "_capture_fixture", None)
        if fixture is not None:
            fixture._start()

    def deactivate_fixture(self, item):
        """Deactivates the ``capsys`` or ``capfd`` fixture of this item, if any."""
        fixture = getattr(item, "_capture_fixture", None)
        if fixture is not None:
            fixture.close()

    def suspend_fixture(self, item):
        fixture = getattr(item, "_capture_fixture", None)
        if fixture is not None:
            fixture._suspend()

    def resume_fixture(self, item):
        fixture = getattr(item, "_capture_fixture", None)
        if fixture is not None:
            fixture._resume()

    # Helper context managers

    @contextlib.contextmanager
    def global_and_fixture_disabled(self):
        """Context manager to temporarily disable global and current fixture capturing."""
        self.suspend()
        try:
            yield
        finally:
            self.resume()

    @contextlib.contextmanager
    def item_capture(self, when, item):
        self.resume_global_capture()
        self.activate_fixture(item)
        try:
            yield
        finally:
            self.deactivate_fixture(item)
            self.suspend_global_capture(in_=False)

        out, err = self.read_global_capture()
        item.add_report_section(when, "stdout", out)
        item.add_report_section(when, "stderr", err)

    # Hooks

    @pytest.hookimpl(hookwrapper=True)
    def pytest_make_collect_report(self, collector):
        if isinstance(collector, pytest.File):
            self.resume_global_capture()
            outcome = yield
            self.suspend_global_capture()
            out, err = self.read_global_capture()
            rep = outcome.get_result()
            if out:
                rep.sections.append(("Captured stdout", out))
            if err:
                rep.sections.append(("Captured stderr", err))
        else:
            yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item):
        self._current_item = item
        yield
        self._current_item = None

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item):
        with self.item_capture("setup", item):
            yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(self, item):
        with self.item_capture("call", item):
            yield

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_teardown(self, item):
        with self.item_capture("teardown", item):
            yield

    @pytest.hookimpl(tryfirst=True)
    def pytest_keyboard_interrupt(self, excinfo):
        self.stop_global_capturing()

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(self, excinfo):
        self.stop_global_capturing()


capture_fixtures = {"capfd", "capfdbinary", "capsys", "capsysbinary"}


def _ensure_only_one_capture_fixture(request, name):
    fixtures = set(request.fixturenames) & capture_fixtures - {name}
    if fixtures:
        fixtures = sorted(fixtures)
        fixtures = fixtures[0] if len(fixtures) == 1 else fixtures
        raise request.raiseerror(
            "cannot use {} and {} at the same time".format(fixtures, name)
        )


@pytest.fixture
def capsys(request):
    """Enable text capturing of writes to ``sys.stdout`` and ``sys.stderr``.

    The captured output is made available via ``capsys.readouterr()`` method
    calls, which return a ``(out, err)`` namedtuple.
    ``out`` and ``err`` will be ``text`` objects.
    """
    _ensure_only_one_capture_fixture(request, "capsys")
    with _install_capture_fixture_on_item(request, SysCapture) as fixture:
        yield fixture


@pytest.fixture
def capsysbinary(request):
    """Enable bytes capturing of writes to ``sys.stdout`` and ``sys.stderr``.

    The captured output is made available via ``capsysbinary.readouterr()``
    method calls, which return a ``(out, err)`` namedtuple.
    ``out`` and ``err`` will be ``bytes`` objects.
    """
    _ensure_only_one_capture_fixture(request, "capsysbinary")
    # Currently, the implementation uses the python3 specific `.buffer`
    # property of CaptureIO.
    if sys.version_info < (3,):
        raise request.raiseerror("capsysbinary is only supported on Python 3")
    with _install_capture_fixture_on_item(request, SysCaptureBinary) as fixture:
        yield fixture


@pytest.fixture
def capfd(request):
    """Enable text capturing of writes to file descriptors ``1`` and ``2``.

    The captured output is made available via ``capfd.readouterr()`` method
    calls, which return a ``(out, err)`` namedtuple.
    ``out`` and ``err`` will be ``text`` objects.
    """
    _ensure_only_one_capture_fixture(request, "capfd")
    if not hasattr(os, "dup"):
        pytest.skip(
            "capfd fixture needs os.dup function which is not available in this system"
        )
    with _install_capture_fixture_on_item(request, FDCapture) as fixture:
        yield fixture


@pytest.fixture
def capfdbinary(request):
    """Enable bytes capturing of writes to file descriptors ``1`` and ``2``.

    The captured output is made available via ``capfd.readouterr()`` method
    calls, which return a ``(out, err)`` namedtuple.
    ``out`` and ``err`` will be ``byte`` objects.
    """
    _ensure_only_one_capture_fixture(request, "capfdbinary")
    if not hasattr(os, "dup"):
        pytest.skip(
            "capfdbinary fixture needs os.dup function which is not available in this system"
        )
    with _install_capture_fixture_on_item(request, FDCaptureBinary) as fixture:
        yield fixture


@contextlib.contextmanager
def _install_capture_fixture_on_item(request, capture_class):
    """
    Context manager which creates a ``CaptureFixture`` instance and "installs" it on
    the item/node of the given request. Used by ``capsys`` and ``capfd``.

    The CaptureFixture is added as attribute of the item because it needs to accessed
    by ``CaptureManager`` during its ``pytest_runtest_*`` hooks.
    """
    request.node._capture_fixture = fixture = CaptureFixture(capture_class, request)
    capmanager = request.config.pluginmanager.getplugin("capturemanager")
    # Need to active this fixture right away in case it is being used by another fixture (setup phase).
    # If this fixture is being used only by a test function (call phase), then we wouldn't need this
    # activation, but it doesn't hurt.
    capmanager.activate_fixture(request.node)
    yield fixture
    fixture.close()
    del request.node._capture_fixture


class CaptureFixture(object):
    """
    Object returned by :py:func:`capsys`, :py:func:`capsysbinary`, :py:func:`capfd` and :py:func:`capfdbinary`
    fixtures.
    """

    def __init__(self, captureclass, request):
        self.captureclass = captureclass
        self.request = request
        self._capture = None
        self._captured_out = self.captureclass.EMPTY_BUFFER
        self._captured_err = self.captureclass.EMPTY_BUFFER

    def _start(self):
        # Start if not started yet
        if getattr(self, "_capture", None) is None:
            self._capture = MultiCapture(
                out=True, err=True, in_=False, Capture=self.captureclass
            )
            self._capture.start_capturing()

    def close(self):
        if self._capture is not None:
            out, err = self._capture.pop_outerr_to_orig()
            self._captured_out += out
            self._captured_err += err
            self._capture.stop_capturing()
            self._capture = None

    def readouterr(self):
        """Read and return the captured output so far, resetting the internal buffer.

        :return: captured content as a namedtuple with ``out`` and ``err`` string attributes
        """
        captured_out, captured_err = self._captured_out, self._captured_err
        if self._capture is not None:
            out, err = self._capture.readouterr()
            captured_out += out
            captured_err += err
        self._captured_out = self.captureclass.EMPTY_BUFFER
        self._captured_err = self.captureclass.EMPTY_BUFFER
        return CaptureResult(captured_out, captured_err)

    def _suspend(self):
        """Suspends this fixture's own capturing temporarily."""
        self._capture.suspend_capturing()

    def _resume(self):
        """Resumes this fixture's own capturing temporarily."""
        self._capture.resume_capturing()

    @contextlib.contextmanager
    def disabled(self):
        """Temporarily disables capture while inside the 'with' block."""
        capmanager = self.request.config.pluginmanager.getplugin("capturemanager")
        with capmanager.global_and_fixture_disabled():
            yield


def safe_text_dupfile(f, mode, default_encoding="UTF8"):
    """ return an open text file object that's a duplicate of f on the
        FD-level if possible.
    """
    encoding = getattr(f, "encoding", None)
    try:
        fd = f.fileno()
    except Exception:
        if "b" not in getattr(f, "mode", "") and hasattr(f, "encoding"):
            # we seem to have a text stream, let's just use it
            return f
    else:
        newfd = os.dup(fd)
        if "b" not in mode:
            mode += "b"
        f = os.fdopen(newfd, mode, 0)  # no buffering
    return EncodedFile(f, encoding or default_encoding)


class EncodedFile(object):
    errors = "strict"  # possibly needed by py3 code (issue555)

    def __init__(self, buffer, encoding):
        self.buffer = buffer
        self.encoding = encoding

    def write(self, obj):
        if isinstance(obj, six.text_type):
            obj = obj.encode(self.encoding, "replace")
        elif _PY3:
            raise TypeError(
                "write() argument must be str, not {}".format(type(obj).__name__)
            )
        self.buffer.write(obj)

    def writelines(self, linelist):
        data = "".join(linelist)
        self.write(data)

    @property
    def name(self):
        """Ensure that file.name is a string."""
        return repr(self.buffer)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "buffer"), name)


CaptureResult = collections.namedtuple("CaptureResult", ["out", "err"])


class MultiCapture(object):
    out = err = in_ = None
    _state = None

    def __init__(self, out=True, err=True, in_=True, Capture=None):
        if in_:
            self.in_ = Capture(0)
        if out:
            self.out = Capture(1)
        if err:
            self.err = Capture(2)

    def __repr__(self):
        return "<MultiCapture out=%r err=%r in_=%r _state=%r _in_suspended=%r>" % (
            self.out,
            self.err,
            self.in_,
            self._state,
            getattr(self, "_in_suspended", "<UNSET>"),
        )

    def start_capturing(self):
        self._state = "started"
        if self.in_:
            self.in_.start()
        if self.out:
            self.out.start()
        if self.err:
            self.err.start()

    def pop_outerr_to_orig(self):
        """ pop current snapshot out/err capture and flush to orig streams. """
        out, err = self.readouterr()
        if out:
            self.out.writeorg(out)
        if err:
            self.err.writeorg(err)
        return out, err

    def suspend_capturing(self, in_=False):
        self._state = "suspended"
        if self.out:
            self.out.suspend()
        if self.err:
            self.err.suspend()
        if in_ and self.in_:
            self.in_.suspend()
            self._in_suspended = True

    def resume_capturing(self):
        self._state = "resumed"
        if self.out:
            self.out.resume()
        if self.err:
            self.err.resume()
        if hasattr(self, "_in_suspended"):
            self.in_.resume()
            del self._in_suspended

    def stop_capturing(self):
        """ stop capturing and reset capturing streams """
        if self._state == "stopped":
            raise ValueError("was already stopped")
        self._state = "stopped"
        if self.out:
            self.out.done()
        if self.err:
            self.err.done()
        if self.in_:
            self.in_.done()

    def readouterr(self):
        """ return snapshot unicode value of stdout/stderr capturings. """
        return CaptureResult(
            self.out.snap() if self.out is not None else "",
            self.err.snap() if self.err is not None else "",
        )


class NoCapture(object):
    EMPTY_BUFFER = None
    __init__ = start = done = suspend = resume = lambda *args: None


class FDCaptureBinary(object):
    """Capture IO to/from a given os-level filedescriptor.

    snap() produces `bytes`
    """

    EMPTY_BUFFER = b""
    _state = None

    def __init__(self, targetfd, tmpfile=None):
        self.targetfd = targetfd
        try:
            self.targetfd_save = os.dup(self.targetfd)
        except OSError:
            self.start = lambda: None
            self.done = lambda: None
        else:
            if targetfd == 0:
                assert not tmpfile, "cannot set tmpfile with stdin"
                tmpfile = open(os.devnull, "r")
                self.syscapture = SysCapture(targetfd)
            else:
                if tmpfile is None:
                    f = TemporaryFile()
                    with f:
                        tmpfile = safe_text_dupfile(f, mode="wb+")
                if targetfd in patchsysdict:
                    self.syscapture = SysCapture(targetfd, tmpfile)
                else:
                    self.syscapture = NoCapture()
            self.tmpfile = tmpfile
            self.tmpfile_fd = tmpfile.fileno()

    def __repr__(self):
        return "<FDCapture %s oldfd=%s _state=%r>" % (
            self.targetfd,
            getattr(self, "targetfd_save", None),
            self._state,
        )

    def start(self):
        """ Start capturing on targetfd using memorized tmpfile. """
        try:
            os.fstat(self.targetfd_save)
        except (AttributeError, OSError):
            raise ValueError("saved filedescriptor not valid anymore")
        os.dup2(self.tmpfile_fd, self.targetfd)
        self.syscapture.start()
        self._state = "started"

    def snap(self):
        self.tmpfile.seek(0)
        res = self.tmpfile.read()
        self.tmpfile.seek(0)
        self.tmpfile.truncate()
        return res

    def done(self):
        """ stop capturing, restore streams, return original capture file,
        seeked to position zero. """
        targetfd_save = self.__dict__.pop("targetfd_save")
        os.dup2(targetfd_save, self.targetfd)
        os.close(targetfd_save)
        self.syscapture.done()
        _attempt_to_close_capture_file(self.tmpfile)
        self._state = "done"

    def suspend(self):
        self.syscapture.suspend()
        os.dup2(self.targetfd_save, self.targetfd)
        self._state = "suspended"

    def resume(self):
        self.syscapture.resume()
        os.dup2(self.tmpfile_fd, self.targetfd)
        self._state = "resumed"

    def writeorg(self, data):
        """ write to original file descriptor. """
        if isinstance(data, six.text_type):
            data = data.encode("utf8")  # XXX use encoding of original stream
        os.write(self.targetfd_save, data)


class FDCapture(FDCaptureBinary):
    """Capture IO to/from a given os-level filedescriptor.

    snap() produces text
    """

    EMPTY_BUFFER = str()

    def snap(self):
        res = super(FDCapture, self).snap()
        enc = getattr(self.tmpfile, "encoding", None)
        if enc and isinstance(res, bytes):
            res = six.text_type(res, enc, "replace")
        return res


class SysCapture(object):

    EMPTY_BUFFER = str()
    _state = None

    def __init__(self, fd, tmpfile=None):
        name = patchsysdict[fd]
        self._old = getattr(sys, name)
        self.name = name
        if tmpfile is None:
            if name == "stdin":
                tmpfile = DontReadFromInput()
            else:
                tmpfile = CaptureIO()
        self.tmpfile = tmpfile

    def __repr__(self):
        return "<SysCapture %s _old=%r, tmpfile=%r _state=%r>" % (
            self.name,
            self._old,
            self.tmpfile,
            self._state,
        )

    def start(self):
        setattr(sys, self.name, self.tmpfile)
        self._state = "started"

    def snap(self):
        res = self.tmpfile.getvalue()
        self.tmpfile.seek(0)
        self.tmpfile.truncate()
        return res

    def done(self):
        setattr(sys, self.name, self._old)
        del self._old
        _attempt_to_close_capture_file(self.tmpfile)
        self._state = "done"

    def suspend(self):
        setattr(sys, self.name, self._old)
        self._state = "suspended"

    def resume(self):
        setattr(sys, self.name, self.tmpfile)
        self._state = "resumed"

    def writeorg(self, data):
        self._old.write(data)
        self._old.flush()


class SysCaptureBinary(SysCapture):
    EMPTY_BUFFER = b""

    def snap(self):
        res = self.tmpfile.buffer.getvalue()
        self.tmpfile.seek(0)
        self.tmpfile.truncate()
        return res


class DontReadFromInput(six.Iterator):
    """Temporary stub class.  Ideally when stdin is accessed, the
    capturing should be turned off, with possibly all data captured
    so far sent to the screen.  This should be configurable, though,
    because in automated test runs it is better to crash than
    hang indefinitely.
    """

    encoding = None

    def read(self, *args):
        raise IOError("reading from stdin while output is captured")

    readline = read
    readlines = read
    __next__ = read

    def __iter__(self):
        return self

    def fileno(self):
        raise UnsupportedOperation("redirected stdin is pseudofile, has no fileno()")

    def isatty(self):
        return False

    def close(self):
        pass

    @property
    def buffer(self):
        if sys.version_info >= (3, 0):
            return self
        else:
            raise AttributeError("redirected stdin has no attribute buffer")


def _colorama_workaround():
    """
    Ensure colorama is imported so that it attaches to the correct stdio
    handles on Windows.

    colorama uses the terminal on import time. So if something does the
    first import of colorama while I/O capture is active, colorama will
    fail in various ways.
    """
    if sys.platform.startswith("win32"):
        try:
            import colorama  # noqa: F401
        except ImportError:
            pass


def _readline_workaround():
    """
    Ensure readline is imported so that it attaches to the correct stdio
    handles on Windows.

    Pdb uses readline support where available--when not running from the Python
    prompt, the readline module is not imported until running the pdb REPL.  If
    running pytest with the --pdb option this means the readline module is not
    imported until after I/O capture has been started.

    This is a problem for pyreadline, which is often used to implement readline
    support on Windows, as it does not attach to the correct handles for stdout
    and/or stdin if they have been redirected by the FDCapture mechanism.  This
    workaround ensures that readline is imported before I/O capture is setup so
    that it can attach to the actual stdin/out for the console.

    See https://github.com/pytest-dev/pytest/pull/1281
    """
    if sys.platform.startswith("win32"):
        try:
            import readline  # noqa: F401
        except ImportError:
            pass


def _py36_windowsconsoleio_workaround(stream):
    """
    Python 3.6 implemented unicode console handling for Windows. This works
    by reading/writing to the raw console handle using
    ``{Read,Write}ConsoleW``.

    The problem is that we are going to ``dup2`` over the stdio file
    descriptors when doing ``FDCapture`` and this will ``CloseHandle`` the
    handles used by Python to write to the console. Though there is still some
    weirdness and the console handle seems to only be closed randomly and not
    on the first call to ``CloseHandle``, or maybe it gets reopened with the
    same handle value when we suspend capturing.

    The workaround in this case will reopen stdio with a different fd which
    also means a different handle by replicating the logic in
    "Py_lifecycle.c:initstdio/create_stdio".

    :param stream: in practice ``sys.stdout`` or ``sys.stderr``, but given
        here as parameter for unittesting purposes.

    See https://github.com/pytest-dev/py/issues/103
    """
    if not sys.platform.startswith("win32") or sys.version_info[:2] < (3, 6):
        return

    # bail out if ``stream`` doesn't seem like a proper ``io`` stream (#2666)
    if not hasattr(stream, "buffer"):
        return

    buffered = hasattr(stream.buffer, "raw")
    raw_stdout = stream.buffer.raw if buffered else stream.buffer

    if not isinstance(raw_stdout, io._WindowsConsoleIO):
        return

    def _reopen_stdio(f, mode):
        if not buffered and mode[0] == "w":
            buffering = 0
        else:
            buffering = -1

        return io.TextIOWrapper(
            open(os.dup(f.fileno()), mode, buffering),
            f.encoding,
            f.errors,
            f.newlines,
            f.line_buffering,
        )

    sys.stdin = _reopen_stdio(sys.stdin, "rb")
    sys.stdout = _reopen_stdio(sys.stdout, "wb")
    sys.stderr = _reopen_stdio(sys.stderr, "wb")


def _attempt_to_close_capture_file(f):
    """Suppress IOError when closing the temporary file used for capturing streams in py27 (#2370)"""
    if six.PY2:
        try:
            f.close()
        except IOError:
            pass
    else:
        f.close()

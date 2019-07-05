"""
exception classes and constants handling test outcomes
as well as functions creating them
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys


class OutcomeException(BaseException):
    """ OutcomeException and its subclass instances indicate and
        contain info about test and collection outcomes.
    """

    def __init__(self, msg=None, pytrace=True):
        BaseException.__init__(self, msg)
        self.msg = msg
        self.pytrace = pytrace

    def __repr__(self):
        if self.msg:
            val = self.msg
            if isinstance(val, bytes):
                val = val.decode("UTF-8", errors="replace")
            return val
        return "<%s instance>" % (self.__class__.__name__,)

    __str__ = __repr__


TEST_OUTCOME = (OutcomeException, Exception)


class Skipped(OutcomeException):
    # XXX hackish: on 3k we fake to live in the builtins
    # in order to have Skipped exception printing shorter/nicer
    __module__ = "builtins"

    def __init__(self, msg=None, pytrace=True, allow_module_level=False):
        OutcomeException.__init__(self, msg=msg, pytrace=pytrace)
        self.allow_module_level = allow_module_level


class Failed(OutcomeException):
    """ raised from an explicit call to pytest.fail() """

    __module__ = "builtins"


class Exit(Exception):
    """ raised for immediate program exits (no tracebacks/summaries)"""

    def __init__(self, msg="unknown reason", returncode=None):
        self.msg = msg
        self.returncode = returncode
        super(Exit, self).__init__(msg)


# exposed helper methods


def exit(msg, returncode=None):
    """
    Exit testing process.

    :param str msg: message to display upon exit.
    :param int returncode: return code to be used when exiting pytest.
    """
    __tracebackhide__ = True
    raise Exit(msg, returncode)


exit.Exception = Exit


def skip(msg="", **kwargs):
    """
    Skip an executing test with the given message.

    This function should be called only during testing (setup, call or teardown) or
    during collection by using the ``allow_module_level`` flag.  This function can
    be called in doctests as well.

    :kwarg bool allow_module_level: allows this function to be called at
        module level, skipping the rest of the module. Default to False.

    .. note::
        It is better to use the :ref:`pytest.mark.skipif ref` marker when possible to declare a test to be
        skipped under certain conditions like mismatching platforms or
        dependencies.
        Similarly, use the ``# doctest: +SKIP`` directive (see `doctest.SKIP
        <https://docs.python.org/3/library/doctest.html#doctest.SKIP>`_)
        to skip a doctest statically.
    """
    __tracebackhide__ = True
    allow_module_level = kwargs.pop("allow_module_level", False)
    if kwargs:
        raise TypeError("unexpected keyword arguments: {}".format(sorted(kwargs)))
    raise Skipped(msg=msg, allow_module_level=allow_module_level)


skip.Exception = Skipped


def fail(msg="", pytrace=True):
    """
    Explicitly fail an executing test with the given message.

    :param str msg: the message to show the user as reason for the failure.
    :param bool pytrace: if false the msg represents the full failure information and no
        python traceback will be reported.
    """
    __tracebackhide__ = True
    raise Failed(msg=msg, pytrace=pytrace)


fail.Exception = Failed


class XFailed(fail.Exception):
    """ raised from an explicit call to pytest.xfail() """


def xfail(reason=""):
    """
    Imperatively xfail an executing test or setup functions with the given reason.

    This function should be called only during testing (setup, call or teardown).

    .. note::
        It is better to use the :ref:`pytest.mark.xfail ref` marker when possible to declare a test to be
        xfailed under certain conditions like known bugs or missing features.
    """
    __tracebackhide__ = True
    raise XFailed(reason)


xfail.Exception = XFailed


def importorskip(modname, minversion=None, reason=None):
    """Imports and returns the requested module ``modname``, or skip the current test
    if the module cannot be imported.

    :param str modname: the name of the module to import
    :param str minversion: if given, the imported module ``__version__`` attribute must be
        at least this minimal version, otherwise the test is still skipped.
    :param str reason: if given, this reason is shown as the message when the module
        cannot be imported.
    """
    import warnings

    __tracebackhide__ = True
    compile(modname, "", "eval")  # to catch syntaxerrors
    should_skip = False

    with warnings.catch_warnings():
        # make sure to ignore ImportWarnings that might happen because
        # of existing directories with the same name we're trying to
        # import but without a __init__.py file
        warnings.simplefilter("ignore")
        try:
            __import__(modname)
        except ImportError:
            # Do not raise chained exception here(#1485)
            should_skip = True
    if should_skip:
        if reason is None:
            reason = "could not import %r" % (modname,)
        raise Skipped(reason, allow_module_level=True)
    mod = sys.modules[modname]
    if minversion is None:
        return mod
    verattr = getattr(mod, "__version__", None)
    if minversion is not None:
        try:
            from pkg_resources import parse_version as pv
        except ImportError:
            raise Skipped(
                "we have a required version for %r but can not import "
                "pkg_resources to parse version strings." % (modname,),
                allow_module_level=True,
            )
        if verattr is None or pv(verattr) < pv(minversion):
            raise Skipped(
                "module %r has __version__ %r, required is: %r"
                % (modname, verattr, minversion),
                allow_module_level=True,
            )
    return mod

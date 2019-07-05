""" monkeypatching and mocking functionality.  """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import sys
import warnings
from contextlib import contextmanager

import six

import pytest
from _pytest.fixtures import fixture
from _pytest.pathlib import Path

RE_IMPORT_ERROR_NAME = re.compile(r"^No module named (.*)$")


@fixture
def monkeypatch():
    """The returned ``monkeypatch`` fixture provides these
    helper methods to modify objects, dictionaries or os.environ::

        monkeypatch.setattr(obj, name, value, raising=True)
        monkeypatch.delattr(obj, name, raising=True)
        monkeypatch.setitem(mapping, name, value)
        monkeypatch.delitem(obj, name, raising=True)
        monkeypatch.setenv(name, value, prepend=False)
        monkeypatch.delenv(name, raising=True)
        monkeypatch.syspath_prepend(path)
        monkeypatch.chdir(path)

    All modifications will be undone after the requesting
    test function or fixture has finished. The ``raising``
    parameter determines if a KeyError or AttributeError
    will be raised if the set/deletion operation has no target.
    """
    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


def resolve(name):
    # simplified from zope.dottedname
    parts = name.split(".")

    used = parts.pop(0)
    found = __import__(used)
    for part in parts:
        used += "." + part
        try:
            found = getattr(found, part)
        except AttributeError:
            pass
        else:
            continue
        # we use explicit un-nesting of the handling block in order
        # to avoid nested exceptions on python 3
        try:
            __import__(used)
        except ImportError as ex:
            # str is used for py2 vs py3
            expected = str(ex).split()[-1]
            if expected == used:
                raise
            else:
                raise ImportError("import error in %s: %s" % (used, ex))
        found = annotated_getattr(found, part, used)
    return found


def annotated_getattr(obj, name, ann):
    try:
        obj = getattr(obj, name)
    except AttributeError:
        raise AttributeError(
            "%r object at %s has no attribute %r" % (type(obj).__name__, ann, name)
        )
    return obj


def derive_importpath(import_path, raising):
    if not isinstance(import_path, six.string_types) or "." not in import_path:
        raise TypeError("must be absolute import path string, not %r" % (import_path,))
    module, attr = import_path.rsplit(".", 1)
    target = resolve(module)
    if raising:
        annotated_getattr(target, attr, ann=module)
    return attr, target


class Notset(object):
    def __repr__(self):
        return "<notset>"


notset = Notset()


class MonkeyPatch(object):
    """ Object returned by the ``monkeypatch`` fixture keeping a record of setattr/item/env/syspath changes.
    """

    def __init__(self):
        self._setattr = []
        self._setitem = []
        self._cwd = None
        self._savesyspath = None

    @contextmanager
    def context(self):
        """
        Context manager that returns a new :class:`MonkeyPatch` object which
        undoes any patching done inside the ``with`` block upon exit:

        .. code-block:: python

            import functools
            def test_partial(monkeypatch):
                with monkeypatch.context() as m:
                    m.setattr(functools, "partial", 3)

        Useful in situations where it is desired to undo some patches before the test ends,
        such as mocking ``stdlib`` functions that might break pytest itself if mocked (for examples
        of this see `#3290 <https://github.com/pytest-dev/pytest/issues/3290>`_.
        """
        m = MonkeyPatch()
        try:
            yield m
        finally:
            m.undo()

    def setattr(self, target, name, value=notset, raising=True):
        """ Set attribute value on target, memorizing the old value.
        By default raise AttributeError if the attribute did not exist.

        For convenience you can specify a string as ``target`` which
        will be interpreted as a dotted import path, with the last part
        being the attribute name.  Example:
        ``monkeypatch.setattr("os.getcwd", lambda: "/")``
        would set the ``getcwd`` function of the ``os`` module.

        The ``raising`` value determines if the setattr should fail
        if the attribute is not already present (defaults to True
        which means it will raise).
        """
        __tracebackhide__ = True
        import inspect

        if value is notset:
            if not isinstance(target, six.string_types):
                raise TypeError(
                    "use setattr(target, name, value) or "
                    "setattr(target, value) with target being a dotted "
                    "import string"
                )
            value = name
            name, target = derive_importpath(target, raising)

        oldval = getattr(target, name, notset)
        if raising and oldval is notset:
            raise AttributeError("%r has no attribute %r" % (target, name))

        # avoid class descriptors like staticmethod/classmethod
        if inspect.isclass(target):
            oldval = target.__dict__.get(name, notset)
        self._setattr.append((target, name, oldval))
        setattr(target, name, value)

    def delattr(self, target, name=notset, raising=True):
        """ Delete attribute ``name`` from ``target``, by default raise
        AttributeError it the attribute did not previously exist.

        If no ``name`` is specified and ``target`` is a string
        it will be interpreted as a dotted import path with the
        last part being the attribute name.

        If ``raising`` is set to False, no exception will be raised if the
        attribute is missing.
        """
        __tracebackhide__ = True
        import inspect

        if name is notset:
            if not isinstance(target, six.string_types):
                raise TypeError(
                    "use delattr(target, name) or "
                    "delattr(target) with target being a dotted "
                    "import string"
                )
            name, target = derive_importpath(target, raising)

        if not hasattr(target, name):
            if raising:
                raise AttributeError(name)
        else:
            oldval = getattr(target, name, notset)
            # Avoid class descriptors like staticmethod/classmethod.
            if inspect.isclass(target):
                oldval = target.__dict__.get(name, notset)
            self._setattr.append((target, name, oldval))
            delattr(target, name)

    def setitem(self, dic, name, value):
        """ Set dictionary entry ``name`` to value. """
        self._setitem.append((dic, name, dic.get(name, notset)))
        dic[name] = value

    def delitem(self, dic, name, raising=True):
        """ Delete ``name`` from dict. Raise KeyError if it doesn't exist.

        If ``raising`` is set to False, no exception will be raised if the
        key is missing.
        """
        if name not in dic:
            if raising:
                raise KeyError(name)
        else:
            self._setitem.append((dic, name, dic.get(name, notset)))
            del dic[name]

    def _warn_if_env_name_is_not_str(self, name):
        """On Python 2, warn if the given environment variable name is not a native str (#4056)"""
        if six.PY2 and not isinstance(name, str):
            warnings.warn(
                pytest.PytestWarning(
                    "Environment variable name {!r} should be str".format(name)
                )
            )

    def setenv(self, name, value, prepend=None):
        """ Set environment variable ``name`` to ``value``.  If ``prepend``
        is a character, read the current environment variable value
        and prepend the ``value`` adjoined with the ``prepend`` character."""
        if not isinstance(value, str):
            warnings.warn(
                pytest.PytestWarning(
                    "Value of environment variable {name} type should be str, but got "
                    "{value!r} (type: {type}); converted to str implicitly".format(
                        name=name, value=value, type=type(value).__name__
                    )
                ),
                stacklevel=2,
            )
            value = str(value)
        if prepend and name in os.environ:
            value = value + prepend + os.environ[name]
        self._warn_if_env_name_is_not_str(name)
        self.setitem(os.environ, name, value)

    def delenv(self, name, raising=True):
        """ Delete ``name`` from the environment. Raise KeyError if it does
        not exist.

        If ``raising`` is set to False, no exception will be raised if the
        environment variable is missing.
        """
        self._warn_if_env_name_is_not_str(name)
        self.delitem(os.environ, name, raising=raising)

    def syspath_prepend(self, path):
        """ Prepend ``path`` to ``sys.path`` list of import locations. """
        from pkg_resources import fixup_namespace_packages

        if self._savesyspath is None:
            self._savesyspath = sys.path[:]
        sys.path.insert(0, str(path))

        # https://github.com/pypa/setuptools/blob/d8b901bc/docs/pkg_resources.txt#L162-L171
        fixup_namespace_packages(str(path))

        # A call to syspathinsert() usually means that the caller wants to
        # import some dynamically created files, thus with python3 we
        # invalidate its import caches.
        # This is especially important when any namespace package is in used,
        # since then the mtime based FileFinder cache (that gets created in
        # this case already) gets not invalidated when writing the new files
        # quickly afterwards.
        if sys.version_info >= (3, 3):
            from importlib import invalidate_caches

            invalidate_caches()

    def chdir(self, path):
        """ Change the current working directory to the specified path.
        Path can be a string or a py.path.local object.
        """
        if self._cwd is None:
            self._cwd = os.getcwd()
        if hasattr(path, "chdir"):
            path.chdir()
        elif isinstance(path, Path):
            # modern python uses the fspath protocol here LEGACY
            os.chdir(str(path))
        else:
            os.chdir(path)

    def undo(self):
        """ Undo previous changes.  This call consumes the
        undo stack. Calling it a second time has no effect unless
        you do more monkeypatching after the undo call.

        There is generally no need to call `undo()`, since it is
        called automatically during tear-down.

        Note that the same `monkeypatch` fixture is used across a
        single test function invocation. If `monkeypatch` is used both by
        the test function itself and one of the test fixtures,
        calling `undo()` will undo all of the changes made in
        both functions.
        """
        for obj, name, value in reversed(self._setattr):
            if value is not notset:
                setattr(obj, name, value)
            else:
                delattr(obj, name)
        self._setattr[:] = []
        for dictionary, name, value in reversed(self._setitem):
            if value is notset:
                try:
                    del dictionary[name]
                except KeyError:
                    pass  # was already deleted, so we have the desired state
            else:
                dictionary[name] = value
        self._setitem[:] = []
        if self._savesyspath is not None:
            sys.path[:] = self._savesyspath
            self._savesyspath = None

        if self._cwd is not None:
            os.chdir(self._cwd)
            self._cwd = None

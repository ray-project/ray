import inspect
import warnings
from collections import namedtuple
from operator import attrgetter

import attr
import six

from ..compat import ascii_escaped
from ..compat import getfslineno
from ..compat import MappingMixin
from ..compat import NOTSET
from _pytest.deprecated import PYTEST_PARAM_UNKNOWN_KWARGS
from _pytest.outcomes import fail
from _pytest.warning_types import PytestUnknownMarkWarning

EMPTY_PARAMETERSET_OPTION = "empty_parameter_set_mark"


def alias(name, warning=None):
    getter = attrgetter(name)

    def warned(self):
        warnings.warn(warning, stacklevel=2)
        return getter(self)

    return property(getter if warning is None else warned, doc="alias for " + name)


def istestfunc(func):
    return (
        hasattr(func, "__call__")
        and getattr(func, "__name__", "<lambda>") != "<lambda>"
    )


def get_empty_parameterset_mark(config, argnames, func):
    from ..nodes import Collector

    requested_mark = config.getini(EMPTY_PARAMETERSET_OPTION)
    if requested_mark in ("", None, "skip"):
        mark = MARK_GEN.skip
    elif requested_mark == "xfail":
        mark = MARK_GEN.xfail(run=False)
    elif requested_mark == "fail_at_collect":
        f_name = func.__name__
        _, lineno = getfslineno(func)
        raise Collector.CollectError(
            "Empty parameter set in '%s' at line %d" % (f_name, lineno + 1)
        )
    else:
        raise LookupError(requested_mark)
    fs, lineno = getfslineno(func)
    reason = "got empty parameter set %r, function %s at %s:%d" % (
        argnames,
        func.__name__,
        fs,
        lineno,
    )
    return mark(reason=reason)


class ParameterSet(namedtuple("ParameterSet", "values, marks, id")):
    @classmethod
    def param(cls, *values, **kwargs):
        marks = kwargs.pop("marks", ())
        if isinstance(marks, MarkDecorator):
            marks = (marks,)
        else:
            assert isinstance(marks, (tuple, list, set))

        id_ = kwargs.pop("id", None)
        if id_ is not None:
            if not isinstance(id_, six.string_types):
                raise TypeError(
                    "Expected id to be a string, got {}: {!r}".format(type(id_), id_)
                )
            id_ = ascii_escaped(id_)

        if kwargs:
            warnings.warn(
                PYTEST_PARAM_UNKNOWN_KWARGS.format(args=sorted(kwargs)), stacklevel=3
            )
        return cls(values, marks, id_)

    @classmethod
    def extract_from(cls, parameterset, force_tuple=False):
        """
        :param parameterset:
            a legacy style parameterset that may or may not be a tuple,
            and may or may not be wrapped into a mess of mark objects

        :param force_tuple:
            enforce tuple wrapping so single argument tuple values
            don't get decomposed and break tests
        """

        if isinstance(parameterset, cls):
            return parameterset
        if force_tuple:
            return cls.param(parameterset)
        else:
            return cls(parameterset, marks=[], id=None)

    @classmethod
    def _for_parametrize(cls, argnames, argvalues, func, config, function_definition):
        if not isinstance(argnames, (tuple, list)):
            argnames = [x.strip() for x in argnames.split(",") if x.strip()]
            force_tuple = len(argnames) == 1
        else:
            force_tuple = False
        parameters = [
            ParameterSet.extract_from(x, force_tuple=force_tuple) for x in argvalues
        ]
        del argvalues

        if parameters:
            # check all parameter sets have the correct number of values
            for param in parameters:
                if len(param.values) != len(argnames):
                    msg = (
                        '{nodeid}: in "parametrize" the number of names ({names_len}):\n'
                        "  {names}\n"
                        "must be equal to the number of values ({values_len}):\n"
                        "  {values}"
                    )
                    fail(
                        msg.format(
                            nodeid=function_definition.nodeid,
                            values=param.values,
                            names=argnames,
                            names_len=len(argnames),
                            values_len=len(param.values),
                        ),
                        pytrace=False,
                    )
        else:
            # empty parameter set (likely computed at runtime): create a single
            # parameter set with NOTSET values, with the "empty parameter set" mark applied to it
            mark = get_empty_parameterset_mark(config, argnames, func)
            parameters.append(
                ParameterSet(values=(NOTSET,) * len(argnames), marks=[mark], id=None)
            )
        return argnames, parameters


@attr.s(frozen=True)
class Mark(object):
    #: name of the mark
    name = attr.ib(type=str)
    #: positional arguments of the mark decorator
    args = attr.ib()  # List[object]
    #: keyword arguments of the mark decorator
    kwargs = attr.ib()  # Dict[str, object]

    def combined_with(self, other):
        """
        :param other: the mark to combine with
        :type other: Mark
        :rtype: Mark

        combines by appending args and merging the mappings
        """
        assert self.name == other.name
        return Mark(
            self.name, self.args + other.args, dict(self.kwargs, **other.kwargs)
        )


@attr.s
class MarkDecorator(object):
    """ A decorator for test functions and test classes.  When applied
    it will create :class:`MarkInfo` objects which may be
    :ref:`retrieved by hooks as item keywords <excontrolskip>`.
    MarkDecorator instances are often created like this::

        mark1 = pytest.mark.NAME              # simple MarkDecorator
        mark2 = pytest.mark.NAME(name1=value) # parametrized MarkDecorator

    and can then be applied as decorators to test functions::

        @mark2
        def test_function():
            pass

    When a MarkDecorator instance is called it does the following:
      1. If called with a single class as its only positional argument and no
         additional keyword arguments, it attaches itself to the class so it
         gets applied automatically to all test cases found in that class.
      2. If called with a single function as its only positional argument and
         no additional keyword arguments, it attaches a MarkInfo object to the
         function, containing all the arguments already stored internally in
         the MarkDecorator.
      3. When called in any other case, it performs a 'fake construction' call,
         i.e. it returns a new MarkDecorator instance with the original
         MarkDecorator's content updated with the arguments passed to this
         call.

    Note: The rules above prevent MarkDecorator objects from storing only a
    single function or class reference as their positional argument with no
    additional keyword or positional arguments.

    """

    mark = attr.ib(validator=attr.validators.instance_of(Mark))

    name = alias("mark.name")
    args = alias("mark.args")
    kwargs = alias("mark.kwargs")

    @property
    def markname(self):
        return self.name  # for backward-compat (2.4.1 had this attr)

    def __eq__(self, other):
        return self.mark == other.mark if isinstance(other, MarkDecorator) else False

    def __repr__(self):
        return "<MarkDecorator %r>" % (self.mark,)

    def with_args(self, *args, **kwargs):
        """ return a MarkDecorator with extra arguments added

        unlike call this can be used even if the sole argument is a callable/class

        :return: MarkDecorator
        """

        mark = Mark(self.name, args, kwargs)
        return self.__class__(self.mark.combined_with(mark))

    def __call__(self, *args, **kwargs):
        """ if passed a single callable argument: decorate it with mark info.
            otherwise add *args/**kwargs in-place to mark information. """
        if args and not kwargs:
            func = args[0]
            is_class = inspect.isclass(func)
            if len(args) == 1 and (istestfunc(func) or is_class):
                store_mark(func, self.mark)
                return func
        return self.with_args(*args, **kwargs)


def get_unpacked_marks(obj):
    """
    obtain the unpacked marks that are stored on an object
    """
    mark_list = getattr(obj, "pytestmark", [])
    if not isinstance(mark_list, list):
        mark_list = [mark_list]
    return normalize_mark_list(mark_list)


def normalize_mark_list(mark_list):
    """
    normalizes marker decorating helpers to mark objects

    :type mark_list: List[Union[Mark, Markdecorator]]
    :rtype: List[Mark]
    """
    extracted = [
        getattr(mark, "mark", mark) for mark in mark_list
    ]  # unpack MarkDecorator
    for mark in extracted:
        if not isinstance(mark, Mark):
            raise TypeError("got {!r} instead of Mark".format(mark))
    return [x for x in extracted if isinstance(x, Mark)]


def store_mark(obj, mark):
    """store a Mark on an object
    this is used to implement the Mark declarations/decorators correctly
    """
    assert isinstance(mark, Mark), mark
    # always reassign name to avoid updating pytestmark
    # in a reference that was only borrowed
    obj.pytestmark = get_unpacked_marks(obj) + [mark]


class MarkGenerator(object):
    """ Factory for :class:`MarkDecorator` objects - exposed as
    a ``pytest.mark`` singleton instance.  Example::

         import pytest
         @pytest.mark.slowtest
         def test_function():
            pass

    will set a 'slowtest' :class:`MarkInfo` object
    on the ``test_function`` object. """

    _config = None
    _markers = set()

    def __getattr__(self, name):
        if name[0] == "_":
            raise AttributeError("Marker name must NOT start with underscore")

        if self._config is not None:
            # We store a set of markers as a performance optimisation - if a mark
            # name is in the set we definitely know it, but a mark may be known and
            # not in the set.  We therefore start by updating the set!
            if name not in self._markers:
                for line in self._config.getini("markers"):
                    # example lines: "skipif(condition): skip the given test if..."
                    # or "hypothesis: tests which use Hypothesis", so to get the
                    # marker name we split on both `:` and `(`.
                    marker = line.split(":")[0].split("(")[0].strip()
                    self._markers.add(marker)

            # If the name is not in the set of known marks after updating,
            # then it really is time to issue a warning or an error.
            if name not in self._markers:
                if self._config.option.strict_markers:
                    fail(
                        "{!r} not found in `markers` configuration option".format(name),
                        pytrace=False,
                    )
                else:
                    warnings.warn(
                        "Unknown pytest.mark.%s - is this a typo?  You can register "
                        "custom marks to avoid this warning - for details, see "
                        "https://docs.pytest.org/en/latest/mark.html" % name,
                        PytestUnknownMarkWarning,
                    )

        return MarkDecorator(Mark(name, (), {}))


MARK_GEN = MarkGenerator()


class NodeKeywords(MappingMixin):
    def __init__(self, node):
        self.node = node
        self.parent = node.parent
        self._markers = {node.name: True}

    def __getitem__(self, key):
        try:
            return self._markers[key]
        except KeyError:
            if self.parent is None:
                raise
            return self.parent.keywords[key]

    def __setitem__(self, key, value):
        self._markers[key] = value

    def __delitem__(self, key):
        raise ValueError("cannot delete key in keywords dict")

    def __iter__(self):
        seen = self._seen()
        return iter(seen)

    def _seen(self):
        seen = set(self._markers)
        if self.parent is not None:
            seen.update(self.parent.keywords)
        return seen

    def __len__(self):
        return len(self._seen())

    def __repr__(self):
        return "<NodeKeywords for node %s>" % (self.node,)


@attr.s(cmp=False, hash=False)
class NodeMarkers(object):
    """
    internal structure for storing marks belonging to a node

    ..warning::

        unstable api

    """

    own_markers = attr.ib(default=attr.Factory(list))

    def update(self, add_markers):
        """update the own markers
        """
        self.own_markers.extend(add_markers)

    def find(self, name):
        """
        find markers in own nodes or parent nodes
        needs a better place
        """
        for mark in self.own_markers:
            if mark.name == name:
                yield mark

    def __iter__(self):
        return iter(self.own_markers)

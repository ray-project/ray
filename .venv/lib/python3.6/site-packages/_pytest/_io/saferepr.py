import pprint

from six.moves import reprlib


def _call_and_format_exception(call, x, *args):
    try:
        # Try the vanilla repr and make sure that the result is a string
        return call(x, *args)
    except Exception as exc:
        exc_name = type(exc).__name__
        try:
            exc_info = str(exc)
        except Exception:
            exc_info = "unknown"
        return '<[%s("%s") raised in repr()] %s object at 0x%x>' % (
            exc_name,
            exc_info,
            x.__class__.__name__,
            id(x),
        )


class SafeRepr(reprlib.Repr):
    """subclass of repr.Repr that limits the resulting size of repr()
    and includes information on exceptions raised during the call.
    """

    def repr(self, x):
        return self._callhelper(reprlib.Repr.repr, self, x)

    def repr_unicode(self, x, level):
        # Strictly speaking wrong on narrow builds
        def repr(u):
            if "'" not in u:
                return u"'%s'" % u
            elif '"' not in u:
                return u'"%s"' % u
            else:
                return u"'%s'" % u.replace("'", r"\'")

        s = repr(x[: self.maxstring])
        if len(s) > self.maxstring:
            i = max(0, (self.maxstring - 3) // 2)
            j = max(0, self.maxstring - 3 - i)
            s = repr(x[:i] + x[len(x) - j :])
            s = s[:i] + "..." + s[len(s) - j :]
        return s

    def repr_instance(self, x, level):
        return self._callhelper(repr, x)

    def _callhelper(self, call, x, *args):
        s = _call_and_format_exception(call, x, *args)
        if len(s) > self.maxsize:
            i = max(0, (self.maxsize - 3) // 2)
            j = max(0, self.maxsize - 3 - i)
            s = s[:i] + "..." + s[len(s) - j :]
        return s


def safeformat(obj):
    """return a pretty printed string for the given object.
    Failing __repr__ functions of user instances will be represented
    with a short exception info.
    """
    return _call_and_format_exception(pprint.pformat, obj)


def saferepr(obj, maxsize=240):
    """return a size-limited safe repr-string for the given object.
    Failing __repr__ functions of user instances will be represented
    with a short exception info and 'saferepr' generally takes
    care to never raise exceptions itself.  This function is a wrapper
    around the Repr/reprlib functionality of the standard 2.6 lib.
    """
    # review exception handling
    srepr = SafeRepr()
    srepr.maxstring = maxsize
    srepr.maxsize = maxsize
    srepr.maxother = 160
    return srepr.repr(obj)

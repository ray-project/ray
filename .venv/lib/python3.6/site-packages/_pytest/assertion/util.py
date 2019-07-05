"""Utilities for assertion debugging"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pprint

import six

import _pytest._code
from ..compat import Sequence
from _pytest import outcomes
from _pytest._io.saferepr import saferepr

# The _reprcompare attribute on the util module is used by the new assertion
# interpretation code and assertion rewriter to detect this plugin was
# loaded and in turn call the hooks defined here as part of the
# DebugInterpreter.
_reprcompare = None


# the re-encoding is needed for python2 repr
# with non-ascii characters (see issue 877 and 1379)
def ecu(s):
    if isinstance(s, bytes):
        return s.decode("UTF-8", "replace")
    else:
        return s


def format_explanation(explanation):
    """This formats an explanation

    Normally all embedded newlines are escaped, however there are
    three exceptions: \n{, \n} and \n~.  The first two are intended
    cover nested explanations, see function and attribute explanations
    for examples (.visit_Call(), visit_Attribute()).  The last one is
    for when one explanation needs to span multiple lines, e.g. when
    displaying diffs.
    """
    explanation = ecu(explanation)
    lines = _split_explanation(explanation)
    result = _format_lines(lines)
    return u"\n".join(result)


def _split_explanation(explanation):
    """Return a list of individual lines in the explanation

    This will return a list of lines split on '\n{', '\n}' and '\n~'.
    Any other newlines will be escaped and appear in the line as the
    literal '\n' characters.
    """
    raw_lines = (explanation or u"").split("\n")
    lines = [raw_lines[0]]
    for values in raw_lines[1:]:
        if values and values[0] in ["{", "}", "~", ">"]:
            lines.append(values)
        else:
            lines[-1] += "\\n" + values
    return lines


def _format_lines(lines):
    """Format the individual lines

    This will replace the '{', '}' and '~' characters of our mini
    formatting language with the proper 'where ...', 'and ...' and ' +
    ...' text, taking care of indentation along the way.

    Return a list of formatted lines.
    """
    result = lines[:1]
    stack = [0]
    stackcnt = [0]
    for line in lines[1:]:
        if line.startswith("{"):
            if stackcnt[-1]:
                s = u"and   "
            else:
                s = u"where "
            stack.append(len(result))
            stackcnt[-1] += 1
            stackcnt.append(0)
            result.append(u" +" + u"  " * (len(stack) - 1) + s + line[1:])
        elif line.startswith("}"):
            stack.pop()
            stackcnt.pop()
            result[stack[-1]] += line[1:]
        else:
            assert line[0] in ["~", ">"]
            stack[-1] += 1
            indent = len(stack) if line.startswith("~") else len(stack) - 1
            result.append(u"  " * indent + line[1:])
    assert len(stack) == 1
    return result


# Provide basestring in python3
try:
    basestring = basestring
except NameError:
    basestring = str


def issequence(x):
    return isinstance(x, Sequence) and not isinstance(x, basestring)


def istext(x):
    return isinstance(x, basestring)


def isdict(x):
    return isinstance(x, dict)


def isset(x):
    return isinstance(x, (set, frozenset))


def isdatacls(obj):
    return getattr(obj, "__dataclass_fields__", None) is not None


def isattrs(obj):
    return getattr(obj, "__attrs_attrs__", None) is not None


def isiterable(obj):
    try:
        iter(obj)
        return not istext(obj)
    except TypeError:
        return False


def assertrepr_compare(config, op, left, right):
    """Return specialised explanations for some operators/operands"""
    width = 80 - 15 - len(op) - 2  # 15 chars indentation, 1 space around op
    left_repr = saferepr(left, maxsize=int(width // 2))
    right_repr = saferepr(right, maxsize=width - len(left_repr))

    summary = u"%s %s %s" % (ecu(left_repr), op, ecu(right_repr))

    verbose = config.getoption("verbose")
    explanation = None
    try:
        if op == "==":
            if istext(left) and istext(right):
                explanation = _diff_text(left, right, verbose)
            else:
                if issequence(left) and issequence(right):
                    explanation = _compare_eq_sequence(left, right, verbose)
                elif isset(left) and isset(right):
                    explanation = _compare_eq_set(left, right, verbose)
                elif isdict(left) and isdict(right):
                    explanation = _compare_eq_dict(left, right, verbose)
                elif type(left) == type(right) and (isdatacls(left) or isattrs(left)):
                    type_fn = (isdatacls, isattrs)
                    explanation = _compare_eq_cls(left, right, verbose, type_fn)
                elif verbose > 0:
                    explanation = _compare_eq_verbose(left, right)
                if isiterable(left) and isiterable(right):
                    expl = _compare_eq_iterable(left, right, verbose)
                    if explanation is not None:
                        explanation.extend(expl)
                    else:
                        explanation = expl
        elif op == "not in":
            if istext(left) and istext(right):
                explanation = _notin_text(left, right, verbose)
    except outcomes.Exit:
        raise
    except Exception:
        explanation = [
            u"(pytest_assertion plugin: representation of details failed.  "
            u"Probably an object has a faulty __repr__.)",
            six.text_type(_pytest._code.ExceptionInfo.from_current()),
        ]

    if not explanation:
        return None

    return [summary] + explanation


def _diff_text(left, right, verbose=0):
    """Return the explanation for the diff between text or bytes.

    Unless --verbose is used this will skip leading and trailing
    characters which are identical to keep the diff minimal.

    If the input are bytes they will be safely converted to text.
    """
    from difflib import ndiff

    explanation = []

    def escape_for_readable_diff(binary_text):
        """
        Ensures that the internal string is always valid unicode, converting any bytes safely to valid unicode.
        This is done using repr() which then needs post-processing to fix the encompassing quotes and un-escape
        newlines and carriage returns (#429).
        """
        r = six.text_type(repr(binary_text)[1:-1])
        r = r.replace(r"\n", "\n")
        r = r.replace(r"\r", "\r")
        return r

    if isinstance(left, bytes):
        left = escape_for_readable_diff(left)
    if isinstance(right, bytes):
        right = escape_for_readable_diff(right)
    if verbose < 1:
        i = 0  # just in case left or right has zero length
        for i in range(min(len(left), len(right))):
            if left[i] != right[i]:
                break
        if i > 42:
            i -= 10  # Provide some context
            explanation = [
                u"Skipping %s identical leading characters in diff, use -v to show" % i
            ]
            left = left[i:]
            right = right[i:]
        if len(left) == len(right):
            for i in range(len(left)):
                if left[-i] != right[-i]:
                    break
            if i > 42:
                i -= 10  # Provide some context
                explanation += [
                    u"Skipping {} identical trailing "
                    u"characters in diff, use -v to show".format(i)
                ]
                left = left[:-i]
                right = right[:-i]
    keepends = True
    if left.isspace() or right.isspace():
        left = repr(str(left))
        right = repr(str(right))
        explanation += [u"Strings contain only whitespace, escaping them using repr()"]
    explanation += [
        line.strip("\n")
        for line in ndiff(left.splitlines(keepends), right.splitlines(keepends))
    ]
    return explanation


def _compare_eq_verbose(left, right):
    keepends = True
    left_lines = repr(left).splitlines(keepends)
    right_lines = repr(right).splitlines(keepends)

    explanation = []
    explanation += [u"-" + line for line in left_lines]
    explanation += [u"+" + line for line in right_lines]

    return explanation


def _compare_eq_iterable(left, right, verbose=0):
    if not verbose:
        return [u"Use -v to get the full diff"]
    # dynamic import to speedup pytest
    import difflib

    try:
        left_formatting = pprint.pformat(left).splitlines()
        right_formatting = pprint.pformat(right).splitlines()
        explanation = [u"Full diff:"]
    except Exception:
        # hack: PrettyPrinter.pformat() in python 2 fails when formatting items that can't be sorted(), ie, calling
        # sorted() on a list would raise. See issue #718.
        # As a workaround, the full diff is generated by using the repr() string of each item of each container.
        left_formatting = sorted(repr(x) for x in left)
        right_formatting = sorted(repr(x) for x in right)
        explanation = [u"Full diff (fallback to calling repr on each item):"]
    explanation.extend(
        line.strip() for line in difflib.ndiff(left_formatting, right_formatting)
    )
    return explanation


def _compare_eq_sequence(left, right, verbose=0):
    explanation = []
    len_left = len(left)
    len_right = len(right)
    for i in range(min(len_left, len_right)):
        if left[i] != right[i]:
            explanation += [u"At index %s diff: %r != %r" % (i, left[i], right[i])]
            break
    len_diff = len_left - len_right

    if len_diff:
        if len_diff > 0:
            dir_with_more = "Left"
            extra = saferepr(left[len_right])
        else:
            len_diff = 0 - len_diff
            dir_with_more = "Right"
            extra = saferepr(right[len_left])

        if len_diff == 1:
            explanation += [u"%s contains one more item: %s" % (dir_with_more, extra)]
        else:
            explanation += [
                u"%s contains %d more items, first extra item: %s"
                % (dir_with_more, len_diff, extra)
            ]
    return explanation


def _compare_eq_set(left, right, verbose=0):
    explanation = []
    diff_left = left - right
    diff_right = right - left
    if diff_left:
        explanation.append(u"Extra items in the left set:")
        for item in diff_left:
            explanation.append(saferepr(item))
    if diff_right:
        explanation.append(u"Extra items in the right set:")
        for item in diff_right:
            explanation.append(saferepr(item))
    return explanation


def _compare_eq_dict(left, right, verbose=0):
    explanation = []
    set_left = set(left)
    set_right = set(right)
    common = set_left.intersection(set_right)
    same = {k: left[k] for k in common if left[k] == right[k]}
    if same and verbose < 2:
        explanation += [u"Omitting %s identical items, use -vv to show" % len(same)]
    elif same:
        explanation += [u"Common items:"]
        explanation += pprint.pformat(same).splitlines()
    diff = {k for k in common if left[k] != right[k]}
    if diff:
        explanation += [u"Differing items:"]
        for k in diff:
            explanation += [saferepr({k: left[k]}) + " != " + saferepr({k: right[k]})]
    extra_left = set_left - set_right
    len_extra_left = len(extra_left)
    if len_extra_left:
        explanation.append(
            u"Left contains %d more item%s:"
            % (len_extra_left, "" if len_extra_left == 1 else "s")
        )
        explanation.extend(
            pprint.pformat({k: left[k] for k in extra_left}).splitlines()
        )
    extra_right = set_right - set_left
    len_extra_right = len(extra_right)
    if len_extra_right:
        explanation.append(
            u"Right contains %d more item%s:"
            % (len_extra_right, "" if len_extra_right == 1 else "s")
        )
        explanation.extend(
            pprint.pformat({k: right[k] for k in extra_right}).splitlines()
        )
    return explanation


def _compare_eq_cls(left, right, verbose, type_fns):
    isdatacls, isattrs = type_fns
    if isdatacls(left):
        all_fields = left.__dataclass_fields__
        fields_to_check = [field for field, info in all_fields.items() if info.compare]
    elif isattrs(left):
        all_fields = left.__attrs_attrs__
        fields_to_check = [field.name for field in all_fields if field.cmp]

    same = []
    diff = []
    for field in fields_to_check:
        if getattr(left, field) == getattr(right, field):
            same.append(field)
        else:
            diff.append(field)

    explanation = []
    if same and verbose < 2:
        explanation.append(u"Omitting %s identical items, use -vv to show" % len(same))
    elif same:
        explanation += [u"Matching attributes:"]
        explanation += pprint.pformat(same).splitlines()
    if diff:
        explanation += [u"Differing attributes:"]
        for field in diff:
            explanation += [
                (u"%s: %r != %r") % (field, getattr(left, field), getattr(right, field))
            ]
    return explanation


def _notin_text(term, text, verbose=0):
    index = text.find(term)
    head = text[:index]
    tail = text[index + len(term) :]
    correct_text = head + tail
    diff = _diff_text(correct_text, text, verbose)
    newdiff = [u"%s is contained here:" % saferepr(term, maxsize=42)]
    for line in diff:
        if line.startswith(u"Skipping"):
            continue
        if line.startswith(u"- "):
            continue
        if line.startswith(u"+ "):
            newdiff.append(u"  " + line[2:])
        else:
            newdiff.append(line)
    return newdiff

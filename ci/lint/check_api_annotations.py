#!/usr/bin/env python

import abc
import types

import ray

IGNORE_PATHS = {".impl.", ".backend.", ".experimental.", ".internal.", ".generated."}


def _fullname(attr):
    """Fully qualified name of an attribute."""
    fullname = ""
    if hasattr(attr, "__module__"):
        fullname += attr.__module__
    if hasattr(attr, "__name__"):
        if fullname:
            fullname += "."
        fullname += attr.__name__
    if not fullname:
        fullname = str(attr)
    return fullname


def _ignore(attr):
    """Whether an attr should be ignored from annotation checking."""
    attr = _fullname(attr)
    if "ray." not in attr or "._" in attr:
        return True
    for path in IGNORE_PATHS:
        if path in attr:
            return True
    return False


# TODO(ekl) also check function and constant definitions.
def verify(symbol, scanned, ok, output, prefix=None):
    """Recursively verify all child symbols of a given module."""
    if not prefix:
        prefix = symbol.__name__ + "."
    if symbol in scanned:
        return
    scanned.add(symbol)
    for child in dir(symbol):
        if child.startswith("_"):
            continue
        attr = getattr(symbol, child)
        if _ignore(attr):
            continue
        if (
            type(attr)
            in [
                type,
                abc.ABCMeta,
                types.FunctionType,
            ]
            and prefix in _fullname(attr)
        ):
            print("Scanning class", attr)
            # Check for magic token added by API annotation decorators.
            av = getattr(attr, "_annotated", None)
            # If not equal, this means the subclass was not annotated but the
            # parent class was.
            if av == attr:
                if attr not in scanned:
                    print("OK:", _fullname(attr))
                    ok.add(attr)
            else:
                output.add(attr)
            scanned.add(attr)
        elif isinstance(attr, types.ModuleType):
            print("Scanning module", attr)
            verify(attr, scanned, ok, output, prefix)
        else:
            print("Not scanning", attr, type(attr))


if __name__ == "__main__":
    import ray.data
    import ray.rllib
    import ray.serve
    import ray.tune
    import ray.train

    output = set()
    ok = set()
    verify(ray.data, set(), ok, output)
    # Sanity check the lint logic.
    assert len(ok) >= 60, len(ok)
    # TODO(ekl) enable it for all modules.
    #    verify(ray.ml, set(), ok, output)
    #    verify(ray.train, set(), ok, output)
    #    verify(ray.serve, set(), ok, output)
    #    verify(ray.rllib, set(), ok, output)
    #    verify(ray.tune, set(), ok, output)
    #    verify(ray, set(), ok, output)

    print("Num ok", len(ok))
    print("Num bad", len(output))
    print("!!! No API stability annotation found for:")
    for x in sorted([_fullname(x) for x in output]):
        print(x)
    if output:
        exit(1)

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import logging
import numpy
import random
import types

from ray.tune import TuneError

logger = logging.getLogger(__name__)


def generate_variants(unresolved_spec):
    """Generates variants from a spec (dict) with unresolved values.

    There are two types of unresolved values:

        Grid search: These define a grid search over values. For example, the
        following grid search values in a spec will produce six distinct
        variants in combination:

            "activation": grid_search(["relu", "tanh"])
            "learning_rate": grid_search([1e-3, 1e-4, 1e-5])

        Lambda functions: These are evaluated to produce a concrete value, and
        can express dependencies or conditional distributions between values.
        They can also be used to express random search (e.g., by calling
        into the `random` or `np` module).

            "cpu": lambda spec: spec.config.num_workers
            "batch_size": lambda spec: random.uniform(1, 1000)

    Finally, to support defining specs in plain JSON / YAML, grid search
    and lambda functions can also be defined alternatively as follows:

        "activation": {"grid_search": ["relu", "tanh"]}
        "cpu": {"eval": "spec.config.num_workers"}
    """
    for resolved_vars, spec in _generate_variants(unresolved_spec):
        assert not _unresolved_values(spec)
        yield format_vars(resolved_vars), spec


def grid_search(values):
    """Convenience method for specifying grid search over a value.

    Arguments:
        values: An iterable whose parameters will be gridded.
    """

    return {"grid_search": values}


class sample_from(object):
    """Specify that tune should sample configuration values from this function.

    The use of function arguments in tune configs must be disambiguated by
    either wrapped the function in tune.eval() or tune.function().

    Arguments:
        func: An callable function to draw a sample from.
    """

    def __init__(self, func):
        self.func = func


class function(object):
    """Wraps `func` to make sure it is not expanded during resolution.

    The use of function arguments in tune configs must be disambiguated by
    either wrapped the function in tune.eval() or tune.function().

    Arguments:
        func: A function literal.
    """

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


_STANDARD_IMPORTS = {
    "random": random,
    "np": numpy,
}

_MAX_RESOLUTION_PASSES = 20


def resolve_nested_dict(nested_dict):
    """Flattens a nested dict by joining keys into tuple of paths.

    Can then be passed into `format_vars`.
    """
    res = {}
    for k, v in nested_dict.items():
        if isinstance(v, dict):
            for k_, v_ in resolve_nested_dict(v).items():
                res[(k, ) + k_] = v_
        else:
            res[(k, )] = v
    return res


def format_vars(resolved_vars):
    out = []
    for path, value in sorted(resolved_vars.items()):
        if path[0] in ["run", "env", "resources_per_trial"]:
            continue  # TrialRunner already has these in the experiment_tag
        pieces = []
        last_string = True
        for k in path[::-1]:
            if isinstance(k, int):
                pieces.append(str(k))
            elif last_string:
                last_string = False
                pieces.append(k)
        pieces.reverse()
        out.append(_clean_value("_".join(pieces)) + "=" + _clean_value(value))
    return ",".join(out)


def _clean_value(value):
    if isinstance(value, float):
        return "{:.5}".format(value)
    else:
        return str(value).replace("/", "_")


def _generate_variants(spec):
    spec = copy.deepcopy(spec)
    unresolved = _unresolved_values(spec)
    if not unresolved:
        yield {}, spec
        return

    grid_vars = []
    lambda_vars = []
    for path, value in unresolved.items():
        if isinstance(value, types.FunctionType):
            lambda_vars.append((path, value))
        else:
            grid_vars.append((path, value))
    grid_vars.sort()

    grid_search = _grid_search_generator(spec, grid_vars)
    for resolved_spec in grid_search:
        resolved_vars = _resolve_lambda_vars(resolved_spec, lambda_vars)
        for resolved, spec in _generate_variants(resolved_spec):
            for path, value in grid_vars:
                resolved_vars[path] = _get_value(spec, path)
            for k, v in resolved.items():
                if (k in resolved_vars and v != resolved_vars[k]
                        and _is_resolved(resolved_vars[k])):
                    raise ValueError(
                        "The variable `{}` could not be unambiguously "
                        "resolved to a single value. Consider simplifying "
                        "your configuration.".format(k))
                resolved_vars[k] = v
            yield resolved_vars, spec


def _assign_value(spec, path, value):
    for k in path[:-1]:
        spec = spec[k]
    spec[path[-1]] = value


def _get_value(spec, path):
    for k in path:
        spec = spec[k]
    return spec


def _resolve_lambda_vars(spec, lambda_vars):
    resolved = {}
    error = True
    num_passes = 0
    while error and num_passes < _MAX_RESOLUTION_PASSES:
        num_passes += 1
        error = False
        for path, fn in lambda_vars:
            try:
                value = fn(_UnresolvedAccessGuard(spec))
            except RecursiveDependencyError as e:
                error = e
            except Exception:
                raise ValueError(
                    "Failed to evaluate expression: {}: {}".format(path, fn) +
                    ". If you meant to pass this as a function literal, use "
                    "tune.function() to escape it.")
            else:
                _assign_value(spec, path, value)
                resolved[path] = value
    if error:
        raise error
    return resolved


def _grid_search_generator(unresolved_spec, grid_vars):
    value_indices = [0] * len(grid_vars)

    def increment(i):
        value_indices[i] += 1
        if value_indices[i] >= len(grid_vars[i][1]):
            value_indices[i] = 0
            if i + 1 < len(value_indices):
                return increment(i + 1)
            else:
                return True
        return False

    if not grid_vars:
        yield unresolved_spec
        return

    while value_indices[-1] < len(grid_vars[-1][1]):
        spec = copy.deepcopy(unresolved_spec)
        for i, (path, values) in enumerate(grid_vars):
            _assign_value(spec, path, values[value_indices[i]])
        yield spec
        if grid_vars:
            done = increment(0)
            if done:
                break


def _is_resolved(v):
    resolved, _ = _try_resolve(v)
    return resolved


def _try_resolve(v):
    if isinstance(v, types.FunctionType):
        raise DeprecationWarning(
            "Function values are ambiguous in Tune "
            "configuations. Either wrap the function with "
            "`tune.function(func)` to specify a function literal, or "
            "`tune.sample_from(func)` to tell Tune to "
            "sample values from the function during variant generation: "
            "{}".format(v))
        return False, v
    elif isinstance(v, sample_from):
        # Function to sample from
        return False, v.func
    elif isinstance(v, dict) and len(v) == 1 and "eval" in v:
        # Lambda function in eval syntax
        return False, lambda spec: eval(
            v["eval"], _STANDARD_IMPORTS, {"spec": spec})
    elif isinstance(v, dict) and len(v) == 1 and "grid_search" in v:
        # Grid search values
        grid_values = v["grid_search"]
        if not isinstance(grid_values, list):
            raise TuneError(
                "Grid search expected list of values, got: {}".format(
                    grid_values))
        return False, grid_values
    return True, v


def _unresolved_values(spec):
    found = {}
    for k, v in spec.items():
        resolved, v = _try_resolve(v)
        if not resolved:
            found[(k, )] = v
        elif isinstance(v, dict):
            # Recurse into a dict
            for (path, value) in _unresolved_values(v).items():
                found[(k, ) + path] = value
        elif isinstance(v, list):
            # Recurse into a list
            for i, elem in enumerate(v):
                for (path, value) in _unresolved_values({i: elem}).items():
                    found[(k, ) + path] = value
    return found


class _UnresolvedAccessGuard(dict):
    def __init__(self, *args, **kwds):
        super(_UnresolvedAccessGuard, self).__init__(*args, **kwds)
        self.__dict__ = self

    def __getattribute__(self, item):
        value = dict.__getattribute__(self, item)
        if not _is_resolved(value):
            raise RecursiveDependencyError(
                "`{}` recursively depends on {}".format(item, value))
        elif isinstance(value, dict):
            return _UnresolvedAccessGuard(value)
        else:
            return value


class RecursiveDependencyError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)

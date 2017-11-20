import copy
import json
import numpy
import os
import random
import types

from ray.tune import TuneError
from ray.tune.trial import Trial
from ray.tune.config_parser import make_parser, json_to_resources


def generate_trials(unresolved_spec, output_path=''):
    """Wraps `generate_variants()` to return a Trial object for each variant.

    See also: generate_variants()

    Arguments:
        unresolved_spec (dict): Experiment spec conforming to the argument
            schema defined in `ray.tune.config_parser`.
        output_path (str): Path where to store experiment outputs.
    """

    if "run" not in unresolved_spec:
        raise TuneError("Must specify `run` in {}".format(unresolved_spec))

    def to_argv(config):
        argv = []
        for k, v in config.items():
            argv.append("--{}".format(k.replace("_", "-")))
            if isinstance(v, str):
                argv.append(v)
            else:
                argv.append(json.dumps(v))
        return argv

    parser = make_parser()
    i = 0
    for _ in range(unresolved_spec.get("repeat", 1)):
        for resolved_vars, spec in generate_variants(unresolved_spec):
            try:
                # Special case the `env` param for RLlib by automatically
                # moving it into the `config` section.
                if "env" in spec:
                    spec["config"] = spec.get("config", {})
                    spec["config"]["env"] = spec["env"]
                    del spec["env"]
                args = parser.parse_args(to_argv(spec))
            except SystemExit:
                raise TuneError("Error parsing args, see above message", spec)
            if resolved_vars:
                experiment_tag = "{}_{}".format(i, resolved_vars)
            else:
                experiment_tag = str(i)
            i += 1
            yield Trial(
                trainable_name=spec["run"],
                config=spec.get("config", {}),
                local_dir=os.path.join(args.local_dir, output_path),
                experiment_tag=experiment_tag,
                resources=json_to_resources(spec.get("resources", {})),
                stopping_criterion=spec.get("stop", {}),
                checkpoint_freq=args.checkpoint_freq,
                restore_path=spec.get("restore"),
                upload_dir=args.upload_dir)


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

        It is also possible to nest the two, e.g. have a lambda function
        return a grid search or vice versa, as long as there are no cyclic
        dependencies between unresolved values.

    Finally, to support defining specs in plain JSON / YAML, grid search
    and lambda functions can also be defined alternatively as follows:

        "activation": {"grid_search": ["relu", "tanh"]}
        "cpu": {"eval": "spec.config.num_workers"}
    """
    for resolved_vars, spec in _generate_variants(unresolved_spec):
        assert not _unresolved_values(spec)
        yield _format_vars(resolved_vars), spec


def grid_search(values):
    """Convenience method for specifying grid search over a value."""

    return {"grid_search": values}


_STANDARD_IMPORTS = {
    "random": random,
    "np": numpy,
}

_MAX_RESOLUTION_PASSES = 20


def _format_vars(resolved_vars):
    out = []
    for path, value in sorted(resolved_vars.items()):
        if path[0] in ["run", "env", "resources"]:
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
        out.append("_".join(pieces) + "=" + str(value))
    return ",".join(out)


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
                if (k in resolved_vars and v != resolved_vars[k] and
                        _is_resolved(resolved_vars[k])):
                    raise ValueError(
                        "The variable `{}` could not be unambiguously "
                        "resolved to a single value. Consider simplifying "
                        "your variable dependencies.".format(k))
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
        # Lambda function
        return False, v
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
            found[(k,)] = v
        elif isinstance(v, dict):
            # Recurse into a dict
            for (path, value) in _unresolved_values(v).items():
                found[(k,) + path] = value
        elif isinstance(v, list):
            # Recurse into a list
            for i, elem in enumerate(v):
                for (path, value) in _unresolved_values({i: elem}).items():
                    found[(k,) + path] = value
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

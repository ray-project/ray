import copy
import numpy
import random
import types


STANDARD_IMPORTS = {
    "random": random,
    "np": numpy,
}

MAX_RESOLUTION_PASSES = 20


# TODO(ekl)
def spec_to_trials(spec):
    for experiment_tag, spec in generate_variants(spec):
        yield Trial(...)


def grid_search(values):
    return {"grid_search": values}


def generate_variants(unresolved_spec):
    for resolved_vars, spec in _generate_variants(unresolved_spec):
        assert not _unresolved_values(spec)
        yield _format_vars(resolved_vars), spec


def _format_vars(resolved_vars):
    out = []
    for path, value in sorted(resolved_vars.items()):
        if path[0] in ["alg", "env", "resources"]:
            continue  # these settings aren't usually search parameters
        pieces = []
        last_string = True
        for k in path[::-1]:
            if type(k) is int:
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
        if type(value) == types.FunctionType:
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
    while error and num_passes < MAX_RESOLUTION_PASSES:
        num_passes += 1
        error = False
        for path, fn in lambda_vars:
            try:
                value = fn(_UnresolvedAccessGuard(spec))
            except RecursionError as e:
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
    if type(v) == types.FunctionType:
        # Lambda function
        return False, v
    elif issubclass(type(v), dict) and len(v) == 1 and "eval" in v:
        # Lambda function in eval syntax
        return False, lambda spec: eval(
            v["eval"], STANDARD_IMPORTS, {"spec": spec})
    elif issubclass(type(v), dict) and len(v) == 1 and "grid_search" in v:
        # Grid search values
        grid_values = v["grid_search"]
        assert type(grid_values) is list, \
            "Grid search expected list of values, got: {}".format(
                grid_values)
        return False, grid_values
    return True, v


def _unresolved_values(spec):
    found = {}
    for k, v in spec.items():
        resolved, v = _try_resolve(v)
        if not resolved:
            found[(k,)] = v
        elif issubclass(type(v), dict):
            # Recurse into a dict
            for (path, value) in _unresolved_values(v).items():
                found[(k,) + path] = value
        elif type(v) == list:
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
            raise RecursionError(
                "`{}` recursively depends on {}".format(item, value))
        elif type(value) is dict:
            return _UnresolvedAccessGuard(value)
        else:
            return value


if __name__ == "__main__":
    def choose_batch_size(spec):
        if spec.config.num_workers > 4:
            return 8
        else:
            return 32

    json_spec = {
        "resources": {
            "cpu": lambda spec: 1 + spec.config.num_workers,
            "gpu": lambda spec: 1 + spec.resources.cpu,
            "driver_cpu": lambda spec: spec.resources.cpu // 2,
        },
        "env": grid_search(["PongDeterministic-v4", "PongNoFrameskip-v4"]),
        "config": {
            "xpu": lambda spec: 1 + spec.resources.gpu,
            "frameskip": lambda spec: "NoFrameskip" not in spec.env,
            "num_workers": grid_search([1, 4, 8]),
            "sgd_batchsize": choose_batch_size,
            "foo": grid_search(["a", "b"]),
            "bar": lambda spec: spec.config.bar2,
            "bar2": lambda spec: spec.config.foo,
            "baz": grid_search([
                lambda spec: str(spec.config.bar) + "2",
                lambda spec: str(spec.config.bar) + "3"]),
            "model": {
                "layers": [
                    lambda spec: spec.config.num_workers * 16,
                    32,
                ],
            },
        },
    }

    for experiment_tag, spec in generate_variants(json_spec):
        print(experiment_tag)

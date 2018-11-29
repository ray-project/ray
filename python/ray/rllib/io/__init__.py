from ray.rllib.io.io_context import IOContext
from ray.rllib.io.json_writer import JsonWriter
from ray.rllib.io.output_writer import OutputWriter, NoopOutput

from six import string_types

__all__ = [
    "OutputWriter",
    "NoopOutput",
    "IOContext",
]


def parse_input_spec(spec):
    return spec


def parse_output_spec(spec, logdir):
    if isinstance(spec, string_types):
        if spec == "json":
            path = logdir
        elif spec.startswith("json:"):
            path = spec[5:]
        else:
            raise ValueError(
                "Output spec must be of format: 'json' or "
                "'json:[scheme:]/path/to/dir', got: {}".format(spec))

        def spec(ioctx):
            return JsonWriter(ioctx, path)

    return spec

import json
import logging
import types

from ray import cloudpickle as cloudpickle
from ray._common.utils import binary_to_hex, hex_to_binary
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


# Marker used by TuneFunctionEncoder/Decoder to embed a cloudpickle blob
# (hex-encoded) inside an otherwise-JSON document for objects that JSON
# cannot represent (e.g. functions). Deserializing such a blob is equivalent
# to ``cloudpickle.loads`` -- i.e. arbitrary code execution -- and must only
# be done for inputs from a trusted source.
_CLOUDPICKLE_FALLBACK_TYPE = "CLOUDPICKLE_FALLBACK"


@DeveloperAPI
class TuneFunctionEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, types.FunctionType):
            return self._to_cloudpickle(obj)
        try:
            return super(TuneFunctionEncoder, self).default(obj)
        except Exception:
            if log_once(f"tune_func_encode:{str(obj)}"):
                logger.debug("Unable to encode. Falling back to cloudpickle.")
            return self._to_cloudpickle(obj)

    def _to_cloudpickle(self, obj):
        return {
            "_type": _CLOUDPICKLE_FALLBACK_TYPE,
            "value": binary_to_hex(cloudpickle.dumps(obj)),
        }


@DeveloperAPI
class TuneFunctionDecoder(json.JSONDecoder):
    """JSON decoder that mirrors :class:`TuneFunctionEncoder`.

    .. warning::

        ``TuneFunctionEncoder`` may embed a cloudpickle blob inside the JSON
        output (under the ``CLOUDPICKLE_FALLBACK`` marker) for objects that
        cannot be JSON-encoded. Deserializing such a blob runs
        ``cloudpickle.loads`` on attacker-controllable bytes, which is
        equivalent to arbitrary code execution.

        For that reason, this decoder **refuses** to expand
        ``CLOUDPICKLE_FALLBACK`` payloads by default and raises ``ValueError``
        instead. Tune-internal callers that load state from a trusted source
        (for example, a path that the same process just wrote) opt in via
        ``TuneFunctionDecoder(allow_cloudpickle=True)``.
    """

    def __init__(self, *args, allow_cloudpickle: bool = False, **kwargs):
        self._allow_cloudpickle = allow_cloudpickle
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if obj.get("_type") == _CLOUDPICKLE_FALLBACK_TYPE:
            if not self._allow_cloudpickle:
                raise ValueError(
                    "Refusing to deserialize an embedded cloudpickle payload "
                    f"({_CLOUDPICKLE_FALLBACK_TYPE!r}) from JSON: this is "
                    "equivalent to executing arbitrary Python code from the "
                    "input. If the input comes from a trusted source, opt in "
                    "explicitly via `TuneFunctionDecoder(allow_cloudpickle=True)`."
                )
            return self._from_cloudpickle(obj)
        return obj

    def _from_cloudpickle(self, obj):
        return cloudpickle.loads(hex_to_binary(obj["value"]))


def _loads_with_cloudpickle(s):
    """Decode a JSON document written by :class:`TuneFunctionEncoder`.

    Accepts ``str`` or ``bytes``/``bytearray`` (decoded as UTF-8), mirroring
    :func:`json.loads`.

    Internal helper: opts in to expanding ``CLOUDPICKLE_FALLBACK`` payloads
    embedded in the JSON document, which executes arbitrary code from those
    payloads. Only call this on input the caller trusts (for example,
    Tune-internal state written by the same process).

    External callers should use ``json.loads(s, cls=TuneFunctionDecoder)``
    instead, which raises ``ValueError`` on any embedded cloudpickle blob.
    """
    if isinstance(s, (bytes, bytearray)):
        s = s.decode("utf-8")
    return TuneFunctionDecoder(allow_cloudpickle=True).decode(s)

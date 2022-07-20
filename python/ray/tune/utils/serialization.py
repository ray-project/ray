import json
import logging
import types

from ray import cloudpickle as cloudpickle
from ray._private.utils import binary_to_hex, hex_to_binary
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


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
            "_type": "CLOUDPICKLE_FALLBACK",
            "value": binary_to_hex(cloudpickle.dumps(obj)),
        }


@DeveloperAPI
class TuneFunctionDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if obj.get("_type") == "CLOUDPICKLE_FALLBACK":
            return self._from_cloudpickle(obj)
        return obj

    def _from_cloudpickle(self, obj):
        return cloudpickle.loads(hex_to_binary(obj["value"]))

import json
import logging
import types
import uuid
from typing import Any

from ray import cloudpickle as cloudpickle
from ray.utils import binary_to_hex, hex_to_binary

logger = logging.getLogger(__name__)


class RawJson:
    def __init__(self, content):
        self.content = content

    def toJSON(self):
        return self.content


class TuneFunctionEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super(TuneFunctionEncoder, self).__init__(*args, **kwargs)
        self._replace_map = {}

    def default(self, obj):
        if isinstance(obj, RawJson):
            key = f"__REPLACE__{uuid.uuid4().hex}__"
            self._replace_map[key] = obj.content
            return key
        elif isinstance(obj, types.FunctionType):
            return self._to_cloudpickle(obj)
        try:
            return super(TuneFunctionEncoder, self).default(obj)
        except Exception:
            logger.debug("Unable to encode. Falling back to cloudpickle.")
            return self._to_cloudpickle(obj)

    def encode(self, o: Any) -> str:
        result = super(TuneFunctionEncoder, self).encode(o)
        for k, v in self._replace_map.items():
            result = result.replace(f'"{k}"', v)
        return result

    def iterencode(self, o: Any, _one_shot: bool = False):
        for result in super(TuneFunctionEncoder, self).iterencode(o):
            for k, v in self._replace_map.items():
                result = result.replace(f'"{k}"', v)
            yield result

    def _to_cloudpickle(self, obj):
        return {
            "_type": "CLOUDPICKLE_FALLBACK",
            "value": binary_to_hex(cloudpickle.dumps(obj))
        }


class TuneFunctionDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if obj.get("_type") == "CLOUDPICKLE_FALLBACK":
            return self._from_cloudpickle(obj)
        return obj

    def _from_cloudpickle(self, obj):
        return cloudpickle.loads(hex_to_binary(obj["value"]))

import base64
import gym
import io
import numpy as np
from typing import Dict
import zlib


def serialize_ndarray(d: np.ndarray) -> str:
    """Pack numpy ndarray into Base64 encoded strings for serialization.

    This function uses numpy.save() instead of pickling to ensure
    compatibility.

    Args:
        d: numpy ndarray.

    Returns:
        b64 escaped string.
    """
    buf = io.BytesIO()
    np.save(buf, d)
    return base64.b64encode(zlib.compress(buf.getvalue())).decode("ascii")


def deserialize_ndarray(s: str) -> np.ndarray:
    return np.load(io.BytesIO(zlib.decompress(base64.b64decode(s))))


def gym_space_to_dict(sp: gym.spaces.Space) -> Dict:
    """Serialize a gym Space into JSON-serializable dict.

    Args:
        sp: gym.spaces.Space

    Returns:
        Serialized JSON string.
    """

    def _box(sp: gym.spaces.Box) -> Dict:
        return {
            "type": "box",
            "low": serialize_ndarray(sp.low),
            "high": serialize_ndarray(sp.high),
            "shape": sp._shape,  # shape is a tuple.
            "dtype": sp.dtype.str,
        }

    def _discrete(sp: gym.spaces.Discrete) -> Dict:
        d = {
            "type": "discrete",
            "n": sp.n,
        }
        # Offset is a relatively new Discrete space feature.
        if hasattr(sp, "start"):
            d["start"] = sp.start
        return d

    def _multi_discrete(sp: gym.spaces.MultiDiscrete) -> Dict:
        return {
            "type": "multi-discrete",
            "nvec": serialize_ndarray(sp.nvec),
            "dtype": sp.dtype.str,
        }

    def _tuple(sp: gym.spaces.Tuple) -> Dict:
        return {
            "type": "tuple",
            "spaces": [gym_space_to_dict(sp) for sp in sp.spaces],
        }

    def _dict(sp: gym.spaces.Dict) -> Dict:
        return {
            "type": "dict",
            "spaces": {k: gym_space_to_dict(sp) for k, sp in sp.spaces.items()},
        }

    if isinstance(sp, gym.spaces.Box):
        return _box(sp)
    elif isinstance(sp, gym.spaces.Discrete):
        return _discrete(sp)
    elif isinstance(sp, gym.spaces.MultiDiscrete):
        return _multi_discrete(sp)
    elif isinstance(sp, gym.spaces.Tuple):
        return _tuple(sp)
    elif isinstance(sp, gym.spaces.Dict):
        return _dict(sp)
    else:
        raise ValueError("Unknown space type for serialization, ", type(sp))


def gym_space_from_dict(d: Dict) -> gym.spaces.Space:
    """De-serialize a dict into gym Space.

    Args:
        str: serialized JSON str.

    Returns:
        De-serialized gym space.
    """

    def __common(d: Dict):
        """Common updates to the dict before we use it to construct spaces"""
        del d["type"]
        if "dtype" in d:
            d["dtype"] = np.dtype(d["dtype"])
        return d

    def _box(d: Dict) -> gym.spaces.Box:
        d.update(
            {
                "low": deserialize_ndarray(d["low"]),
                "high": deserialize_ndarray(d["high"]),
            }
        )
        return gym.spaces.Box(**__common(d))

    def _discrete(d: Dict) -> gym.spaces.Discrete:
        return gym.spaces.Discrete(**__common(d))

    def _multi_discrete(d: Dict) -> gym.spaces.Discrete:
        d.update(
            {
                "nvec": deserialize_ndarray(d["nvec"]),
            }
        )
        return gym.spaces.MultiDiscrete(**__common(d))

    def _tuple(d: Dict) -> gym.spaces.Discrete:
        spaces = [gym_space_from_dict(sp) for sp in d["spaces"]]
        return gym.spaces.Tuple(spaces=spaces)

    def _dict(d: Dict) -> gym.spaces.Discrete:
        spaces = {k: gym_space_from_dict(sp) for k, sp in d["spaces"].items()}
        return gym.spaces.Dict(spaces=spaces)

    type = d["type"]
    if type == "box":
        return _box(d)
    elif type == "discrete":
        return _discrete(d)
    elif type == "multi-discrete":
        return _multi_discrete(d)
    elif type == "tuple":
        return _tuple(d)
    elif type == "dict":
        return _dict(d)
    else:
        raise ValueError("Unknown space type for de-serialization, ", type)

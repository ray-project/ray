import base64
import gym
import io
import numpy as np
from typing import Dict
import zlib

from ray.rllib.utils.annotations import DeveloperAPI


def _serialize_ndarray(array: np.ndarray) -> str:
    """Pack numpy ndarray into Base64 encoded strings for serialization.

    This function uses numpy.save() instead of pickling to ensure
    compatibility.

    Args:
        array: numpy ndarray.

    Returns:
        b64 escaped string.
    """
    buf = io.BytesIO()
    np.save(buf, array)
    return base64.b64encode(zlib.compress(buf.getvalue())).decode("ascii")


def _deserialize_ndarray(b64_string: str) -> np.ndarray:
    """Unpack b64 escaped string into numpy ndarray.

    This function assumes the unescaped bytes are of npy format.

    Args:
        b64_string: Base64 escaped string.

    Returns:
        numpy ndarray.
    """
    return np.load(io.BytesIO(zlib.decompress(base64.b64decode(b64_string))))


@DeveloperAPI
def gym_space_to_dict(space: gym.spaces.Space) -> Dict:
    """Serialize a gym Space into JSON-serializable dict.

    Args:
        space: gym.spaces.Space

    Returns:
        Serialized JSON string.
    """

    def _box(sp: gym.spaces.Box) -> Dict:
        return {
            "space": "box",
            "low": _serialize_ndarray(sp.low),
            "high": _serialize_ndarray(sp.high),
            "shape": sp._shape,  # shape is a tuple.
            "dtype": sp.dtype.str,
        }

    def _discrete(sp: gym.spaces.Discrete) -> Dict:
        d = {
            "space": "discrete",
            "n": sp.n,
        }
        # Offset is a relatively new Discrete space feature.
        if hasattr(sp, "start"):
            d["start"] = sp.start
        return d

    def _multi_discrete(sp: gym.spaces.MultiDiscrete) -> Dict:
        return {
            "space": "multi-discrete",
            "nvec": _serialize_ndarray(sp.nvec),
            "dtype": sp.dtype.str,
        }

    def _tuple(sp: gym.spaces.Tuple) -> Dict:
        return {
            "space": "tuple",
            "spaces": [gym_space_to_dict(sp) for sp in sp.spaces],
        }

    def _dict(sp: gym.spaces.Dict) -> Dict:
        return {
            "space": "dict",
            "spaces": {k: gym_space_to_dict(sp) for k, sp in sp.spaces.items()},
        }

    if isinstance(space, gym.spaces.Box):
        return _box(space)
    elif isinstance(space, gym.spaces.Discrete):
        return _discrete(space)
    elif isinstance(space, gym.spaces.MultiDiscrete):
        return _multi_discrete(space)
    elif isinstance(space, gym.spaces.Tuple):
        return _tuple(space)
    elif isinstance(space, gym.spaces.Dict):
        return _dict(space)
    else:
        raise ValueError("Unknown space type for serialization, ", type(space))


@DeveloperAPI
def gym_space_from_dict(d: Dict) -> gym.spaces.Space:
    """De-serialize a dict into gym Space.

    Args:
        str: serialized JSON str.

    Returns:
        De-serialized gym space.
    """

    def __common(d: Dict):
        """Common updates to the dict before we use it to construct spaces"""
        del d["space"]
        if "dtype" in d:
            d["dtype"] = np.dtype(d["dtype"])
        return d

    def _box(d: Dict) -> gym.spaces.Box:
        d.update(
            {
                "low": _deserialize_ndarray(d["low"]),
                "high": _deserialize_ndarray(d["high"]),
            }
        )
        return gym.spaces.Box(**__common(d))

    def _discrete(d: Dict) -> gym.spaces.Discrete:
        return gym.spaces.Discrete(**__common(d))

    def _multi_discrete(d: Dict) -> gym.spaces.Discrete:
        d.update(
            {
                "nvec": _deserialize_ndarray(d["nvec"]),
            }
        )
        return gym.spaces.MultiDiscrete(**__common(d))

    def _tuple(d: Dict) -> gym.spaces.Discrete:
        spaces = [gym_space_from_dict(sp) for sp in d["spaces"]]
        return gym.spaces.Tuple(spaces=spaces)

    def _dict(d: Dict) -> gym.spaces.Discrete:
        spaces = {k: gym_space_from_dict(sp) for k, sp in d["spaces"].items()}
        return gym.spaces.Dict(spaces=spaces)

    space_map = {
        "box": _box,
        "discrete": _discrete,
        "multi-discrete": _multi_discrete,
        "tuple": _tuple,
        "dict": _dict,
    }

    space_type = d["space"]
    if space_type not in space_map:
        raise ValueError("Unknown space type for de-serialization, ", space_type)

    return space_map[space_type](d)

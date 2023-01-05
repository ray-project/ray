import base64
import numpy as np
import io
import zlib
from typing import Dict

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.gym import try_import_gymnasium_and_gym
from ray.rllib.utils.spaces.flexdict import FlexDict
from ray.rllib.utils.spaces.repeated import Repeated
from ray.rllib.utils.spaces.simplex import Simplex

gym, old_gym = try_import_gymnasium_and_gym()

old_gym_text_class = None
if old_gym:
    old_gym_text_class = getattr(old_gym.spaces, "Text", None)


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
    """Serialize a gym Space into a JSON-serializable dict.

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

    def _multi_binary(sp: gym.spaces.MultiBinary) -> Dict:
        return {
            "space": "multi-binary",
            "n": sp.n,
            "dtype": sp.dtype.str,
        }

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

    def _simplex(sp: Simplex) -> Dict:
        return {
            "space": "simplex",
            "shape": sp._shape,  # shape is a tuple.
            "concentration": sp.concentration,
            "dtype": sp.dtype.str,
        }

    def _repeated(sp: Repeated) -> Dict:
        return {
            "space": "repeated",
            "child_space": gym_space_to_dict(sp.child_space),
            "max_len": sp.max_len,
        }

    def _flex_dict(sp: FlexDict) -> Dict:
        d = {
            "space": "flex_dict",
        }
        for k, s in sp.spaces:
            d[k] = gym_space_to_dict(s)
        return d

    def _text(sp: "gym.spaces.Text") -> Dict:
        # Note (Kourosh): This only works in gym >= 0.25.0
        charset = getattr(sp, "character_set", None)
        if charset is None:
            charset = getattr(sp, "charset", None)
        if charset is None:
            raise ValueError(
                "Text space must have a character_set or charset attribute"
            )
        return {
            "space": "text",
            "min_length": sp.min_length,
            "max_length": sp.max_length,
            "charset": charset,
        }

    if isinstance(space, gym.spaces.Box):
        return _box(space)
    elif isinstance(space, gym.spaces.Discrete):
        return _discrete(space)
    elif isinstance(space, gym.spaces.MultiBinary):
        return _multi_binary(space)
    elif isinstance(space, gym.spaces.MultiDiscrete):
        return _multi_discrete(space)
    elif isinstance(space, gym.spaces.Tuple):
        return _tuple(space)
    elif isinstance(space, gym.spaces.Dict):
        return _dict(space)
    elif isinstance(space, gym.spaces.Text):
        return _text(space)
    elif isinstance(space, Simplex):
        return _simplex(space)
    elif isinstance(space, Repeated):
        return _repeated(space)
    elif isinstance(space, FlexDict):
        return _flex_dict(space)
    # Old gym Spaces.
    elif old_gym and isinstance(space, old_gym.spaces.Box):
        return _box(space)
    elif old_gym and isinstance(space, old_gym.spaces.Discrete):
        return _discrete(space)
    elif old_gym and isinstance(space, old_gym.spaces.MultiDiscrete):
        return _multi_discrete(space)
    elif old_gym and isinstance(space, old_gym.spaces.Tuple):
        return _tuple(space)
    elif old_gym and isinstance(space, old_gym.spaces.Dict):
        return _dict(space)
    elif old_gym and old_gym_text_class and isinstance(space, old_gym_text_class):
        return _text(space)
    else:
        raise ValueError("Unknown space type for serialization, ", type(space))


@DeveloperAPI
def space_to_dict(space: gym.spaces.Space) -> Dict:
    d = {"space": gym_space_to_dict(space)}
    if "original_space" in space.__dict__:
        d["original_space"] = gym_space_to_dict(space.original_space)
    return d


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
        ret = d.copy()
        del ret["space"]
        if "dtype" in ret:
            ret["dtype"] = np.dtype(ret["dtype"])
        return ret

    def _box(d: Dict) -> gym.spaces.Box:
        ret = d.copy()
        ret.update(
            {
                "low": _deserialize_ndarray(d["low"]),
                "high": _deserialize_ndarray(d["high"]),
            }
        )
        return gym.spaces.Box(**__common(ret))

    def _discrete(d: Dict) -> gym.spaces.Discrete:
        return gym.spaces.Discrete(**__common(d))

    def _multi_binary(d: Dict) -> gym.spaces.MultiBinary:
        return gym.spaces.MultiBinary(**__common(d))

    def _multi_discrete(d: Dict) -> gym.spaces.MultiDiscrete:
        ret = d.copy()
        ret.update(
            {
                "nvec": _deserialize_ndarray(ret["nvec"]),
            }
        )
        return gym.spaces.MultiDiscrete(**__common(ret))

    def _tuple(d: Dict) -> gym.spaces.Discrete:
        spaces = [gym_space_from_dict(sp) for sp in d["spaces"]]
        return gym.spaces.Tuple(spaces=spaces)

    def _dict(d: Dict) -> gym.spaces.Discrete:
        spaces = {k: gym_space_from_dict(sp) for k, sp in d["spaces"].items()}
        return gym.spaces.Dict(spaces=spaces)

    def _simplex(d: Dict) -> Simplex:
        return Simplex(**__common(d))

    def _repeated(d: Dict) -> Repeated:
        child_space = gym_space_from_dict(d["child_space"])
        return Repeated(child_space=child_space, max_len=d["max_len"])

    def _flex_dict(d: Dict) -> FlexDict:
        spaces = {k: gym_space_from_dict(s) for k, s in d.items() if k != "space"}
        return FlexDict(spaces=spaces)

    def _text(d: Dict) -> "gym.spaces.Text":
        return gym.spaces.Text(**__common(d))

    space_map = {
        "box": _box,
        "discrete": _discrete,
        "multi-binary": _multi_binary,
        "multi-discrete": _multi_discrete,
        "tuple": _tuple,
        "dict": _dict,
        "simplex": _simplex,
        "repeated": _repeated,
        "flex_dict": _flex_dict,
        "text": _text,
    }

    space_type = d["space"]
    if space_type not in space_map:
        raise ValueError("Unknown space type for de-serialization, ", space_type)

    return space_map[space_type](d)


@DeveloperAPI
def space_from_dict(d: Dict) -> gym.spaces.Space:
    space = gym_space_from_dict(d["space"])
    if "original_space" in d:
        space.original_space = gym_space_from_dict(d["original_space"])
    return space

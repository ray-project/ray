import base64
import importlib
import io
import zlib
from collections import OrderedDict
from typing import Any, Dict, Optional, Sequence, Type, Union

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.error import NotSerializable
from ray.rllib.utils.spaces.flexdict import FlexDict
from ray.rllib.utils.spaces.repeated import Repeated
from ray.rllib.utils.spaces.simplex import Simplex

NOT_SERIALIZABLE = "__not_serializable__"


@DeveloperAPI
def convert_numpy_to_python_primitives(obj: Any):
    """Convert an object that is a numpy type to a python type.

    If the object is not a numpy type, it is returned unchanged.

    Args:
        obj: The object to convert.
    """
    if isinstance(obj, dict):
        return {
            key: convert_numpy_to_python_primitives(val) for key, val in obj.items()
        }
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_to_python_primitives(val) for val in obj)
    elif isinstance(obj, list):
        return [convert_numpy_to_python_primitives(val) for val in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.str_):
        return str(obj)
    elif isinstance(obj, np.ndarray):
        ret = obj.tolist()
        for i, v in enumerate(ret):
            ret[i] = convert_numpy_to_python_primitives(v)
        return ret
    else:
        return obj


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
    return np.load(
        io.BytesIO(zlib.decompress(base64.b64decode(b64_string))), allow_pickle=True
    )


@DeveloperAPI
def gym_space_to_dict(space: gym.spaces.Space) -> Dict:
    """Serialize a gym Space into a JSON-serializable dict.

    Args:
        space: gym.spaces.Space

    Returns:
        Serialized JSON string.
    """
    if space is None:
        return None

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
            "n": int(sp.n),
        }
        # Offset is a relatively new Discrete space feature.
        if hasattr(sp, "start"):
            d["start"] = int(sp.start)
        return d

    def _multi_binary(sp: gym.spaces.MultiBinary) -> Dict:
        return {
            "space": "multi-binary",
            "n": sp.n,
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
    else:
        raise ValueError("Unknown space type for serialization, ", type(space))


@DeveloperAPI
def space_to_dict(space: gym.spaces.Space) -> Dict:
    d = {"space": gym_space_to_dict(space)}
    if "original_space" in space.__dict__:
        d["original_space"] = space_to_dict(space.original_space)
    return d


@DeveloperAPI
def gym_space_from_dict(d: Dict) -> gym.spaces.Space:
    """De-serialize a dict into gym Space.

    Args:
        str: serialized JSON str.

    Returns:
        De-serialized gym space.
    """
    if d is None:
        return None

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
        # We need to always use an OrderedDict here to cover the following two ways, by
        # which a user might construct a Dict space originally. We need to restore this
        # original Dict space with the exact order of keys the user intended to.
        # - User provides an OrderedDict inside the gym.spaces.Dict constructor ->
        #  gymnasium should NOT further sort the keys. The same (user-provided) order
        #  must be restored.
        # - User provides a simple dict inside the gym.spaces.Dict constructor ->
        #  By its API definition, gymnasium automatically sorts all keys alphabetically.
        #  The same (alphabetical) order must thus be restored.
        spaces = OrderedDict(
            {k: gym_space_from_dict(sp) for k, sp in d["spaces"].items()}
        )
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
        assert "space" in d["original_space"]
        if isinstance(d["original_space"]["space"], str):
            # For backward compatibility reasons, if d["original_space"]["space"]
            # is a string, this original space was serialized by gym_space_to_dict.
            space.original_space = gym_space_from_dict(d["original_space"])
        else:
            # Otherwise, this original space was serialized by space_to_dict.
            space.original_space = space_from_dict(d["original_space"])
    return space


@DeveloperAPI
def check_if_args_kwargs_serializable(args: Sequence[Any], kwargs: Dict[str, Any]):
    """Check if parameters to a function are serializable by ray.

    Args:
        args: arguments to be checked.
        kwargs: keyword arguments to be checked.

    Raises:
        NoteSerializable if either args are kwargs are not serializable
            by ray.
    """
    for arg in args:
        try:
            # if the object is truly serializable we should be able to
            # ray.put and ray.get it.
            ray.get(ray.put(arg))
        except TypeError as e:
            raise NotSerializable(
                "RLModule constructor arguments must be serializable. "
                f"Found non-serializable argument: {arg}.\n"
                f"Original serialization error: {e}"
            )
    for k, v in kwargs.items():
        try:
            # if the object is truly serializable we should be able to
            # ray.put and ray.get it.
            ray.get(ray.put(v))
        except TypeError as e:
            raise NotSerializable(
                "RLModule constructor arguments must be serializable. "
                f"Found non-serializable keyword argument: {k} = {v}.\n"
                f"Original serialization error: {e}"
            )


@DeveloperAPI
def serialize_type(type_: Union[Type, str]) -> str:
    """Converts a type into its full classpath ([module file] + "." + [class name]).

    Args:
        type_: The type to convert.

    Returns:
        The full classpath of the given type, e.g. "ray.rllib.algorithms.ppo.PPOConfig".
    """
    # TODO (avnishn): find a way to incorporate the tune registry here.
    # Already serialized.
    if isinstance(type_, str):
        return type_

    return type_.__module__ + "." + type_.__qualname__


@DeveloperAPI
def deserialize_type(
    module: Union[str, Type], error: bool = False
) -> Optional[Union[str, Type]]:
    """Resolves a class path to a class.
    If the given module is already a class, it is returned as is.
    If the given module is a string, it is imported and the class is returned.

    Args:
        module: The classpath (str) or type to resolve.
        error: Whether to throw a ValueError if `module` could not be resolved into
            a class. If False and `module` is not resolvable, returns None.

    Returns:
        The resolved class or `module` (if `error` is False and no resolution possible).

    Raises:
        ValueError: If `error` is True and `module` cannot be resolved.
    """
    # Already a class, return as-is.
    if isinstance(module, type):
        return module
    # A string.
    elif isinstance(module, str):
        # Try interpreting (as classpath) and importing the given module.
        try:
            module_path, class_name = module.rsplit(".", 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        # Module not found OR not a module (but a registered string?).
        except (ModuleNotFoundError, ImportError, AttributeError, ValueError) as e:
            # Ignore if error=False.
            if error:
                raise ValueError(
                    f"Could not deserialize the given classpath `module={module}` into "
                    "a valid python class! Make sure you have all necessary pip "
                    "packages installed and all custom modules are in your "
                    "`PYTHONPATH` env variable."
                ) from e
    else:
        raise ValueError(f"`module` ({module} must be type or string (classpath)!")

    return module

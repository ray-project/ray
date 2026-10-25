import io
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


import ray._private.utils
import ray.cloudpickle as pickle
import ray.exceptions
from ray._private import ray_constants
from ray.util import inspect_serializability

logger = logging.getLogger(__name__)
ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION = ray_constants.env_bool(
    "RAY_allow_out_of_band_object_ref_serialization", True
)


def pickle_dumps(obj: Any, error_msg: str):
    """Wrap cloudpickle.dumps to provide better error message
    when the object is not serializable.
    """
    try:
        return pickle.dumps(obj)
    except (TypeError, ray.exceptions.OufOfBandObjectRefSerializationException) as e:
        sio = io.StringIO()
        inspect_serializability(obj, print_file=sio)
        msg = f"{error_msg}:\n{sio.getvalue()}"
        if isinstance(e, TypeError):
            raise TypeError(msg) from e
        else:
            raise ray.exceptions.OufOfBandObjectRefSerializationException(msg)

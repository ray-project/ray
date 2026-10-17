import io
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


import ray._private.utils
import ray._private.worker
import ray.cloudpickle as pickle
import ray.exceptions
from ray._private import ray_constants
from ray.util import inspect_serializability, serialization_addons

logger = logging.getLogger(__name__)
ALLOW_OUT_OF_BAND_OBJECT_REF_SERIALIZATION = ray_constants.env_bool(
    "RAY_allow_out_of_band_object_ref_serialization", True
)


def pickle_dumps(obj: Any, error_msg: str):
    """Wrap cloudpickle.dumps to provide better error message
    when the object is not serializable.
    """
    # Libraries imported since the last pickling get their serializers. The
    # worker's serialization context registers them, so it must exist: a
    # function can be pickled before anything is serialized.
    ray._private.worker.global_worker.get_serialization_context()
    serialization_addons.apply_pending()
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

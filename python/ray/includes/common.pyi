# source: common.pxi
from typing import TypeVar, Union

_GCO = TypeVar("_GCO",bound=GcsClientOptions)
class GcsClientOptions:
    """Cython wrapper class of C++ `ray::gcs::GcsClientOptions`."""

    @classmethod
    def create(
        cls: type[_GCO], gcs_address: Union[str, bytes], cluster_id_hex: Union[str, bytes], allow_cluster_id_nil: bool, fetch_cluster_id_if_nil: bool
    ) -> _GCO:
        """
        Creates a GcsClientOption with a maybe-Nil cluster_id, and may fetch from GCS.
        """
        ...

# source: common.pxi
from typing import TypeVar, Union

_GCO = TypeVar("_GCO",bound=GcsClientOptions)
class GcsClientOptions:
    """Cython wrapper class of C++ `ray::gcs::GcsClientOptions`."""

    @classmethod
    def create(
        cls:type[_GCO], gcs_address:Union[str,bytes], cluster_id_hex:Union[str,bytes], allow_cluster_id_nil:bool, fetch_cluster_id_if_nil:bool
    )->_GCO:
        """
        Creates a GcsClientOption with a maybe-Nil cluster_id, and may fetch from GCS.
        """
        ...

WORKER_PROCESS_SETUP_HOOK_KEY_NAME_GCS:str
RESOURCE_UNIT_SCALING:int
IMPLICIT_RESOURCE_PREFIX:str
STREAMING_GENERATOR_RETURN:int
GCS_AUTOSCALER_STATE_NAMESPACE:str
GCS_AUTOSCALER_V2_ENABLED_KEY:str
GCS_AUTOSCALER_CLUSTER_CONFIG_KEY:str
GCS_PID_KEY:str

# Ray node label related constants form src/ray/common/constants.h
NODE_TYPE_NAME_ENV:str
NODE_MARKET_TYPE_ENV:str
NODE_REGION_ENV:str
NODE_ZONE_ENV:str

RAY_NODE_ACCELERATOR_TYPE_KEY:str
RAY_NODE_MARKET_TYPE_KEY:str
RAY_NODE_REGION_KEY:str
RAY_NODE_ZONE_KEY:str
RAY_NODE_GROUP_KEY:str

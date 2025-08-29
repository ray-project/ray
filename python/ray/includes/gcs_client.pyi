from asyncio import Future
from typing import Dict, List, Optional, Sequence

import ray
from ray.core.generated import gcs_pb2, autoscaler_pb2
from ray.core.generated.gcs_service_pb2 import GetAllResourceUsageReply
from ray.includes.unique_ids import NodeID, ActorID, JobID


class InnerGcsClient:

    @staticmethod
    def standalone(gcs_address: str,
                   cluster_id: Optional[str],
                   timeout_ms: int) -> "InnerGcsClient": ...

    @property
    def address(self) -> str: ...

    @property
    def cluster_id(self) -> ray.ClusterID: ...

    #############################################################
    # Internal KV sync methods
    #############################################################
    def internal_kv_get(
        self, key: bytes, namespace:Optional[bytes]=None, timeout: Optional[int | float]=None
    ) -> Optional[bytes]: ...

    def internal_kv_multi_get(
        self, keys: List[bytes], namespace:Optional[bytes]=None, timeout: Optional[int | float]=None
    ) -> Dict[bytes, bytes]: ...

    def internal_kv_put(self, key: bytes, value: bytes, overwrite:bool=False,
                        namespace:Optional[bytes]=None, timeout: Optional[int | float]=None) -> int:
        """
        Returns 1 if the key is newly added, 0 if the key is overwritten.
        """
        ...

    def internal_kv_del(self, key: bytes, del_by_prefix:bool,
                        namespace:Optional[bytes]=None, timeout: Optional[int | float]=None) -> int:
        """
        Returns number of keys deleted.
        """
        ...

    def internal_kv_keys(
        self, prefix: bytes, namespace:Optional[bytes]=None, timeout: Optional[int | float]=None
    ) -> List[bytes]:...

    def internal_kv_exists(self, key: bytes, namespace:Optional[bytes]=None, timeout: Optional[int | float]=None) -> bool: ...

    #############################################################
    # Internal KV async methods
    #############################################################

    def async_internal_kv_get(
        self, key: bytes, namespace:Optional[bytes]=None, timeout: Optional[int | float]=None
    ) -> Future[Optional[bytes]]: ...

    def async_internal_kv_multi_get(
        self, keys: List[bytes], namespace:Optional[bytes]=None, timeout: Optional[int | float]=None
    ) -> Future[Dict[bytes, bytes]]: ...

    def async_internal_kv_put(
        self, key: bytes, value: bytes, overwrite:bool=False, namespace:Optional[bytes]=None,
        timeout: Optional[int | float]=None
    ) -> Future[bool]: ...

    def async_internal_kv_del(self, key: bytes, del_by_prefix:bool,
                              namespace:Optional[bytes]=None, timeout: Optional[int | float]=None) -> Future[int]: ...

    def async_internal_kv_keys(self, prefix: bytes, namespace:Optional[bytes]=None, timeout: Optional[int | float]=None
                               ) -> Future[List[bytes]]: ...

    def async_internal_kv_exists(self, key: bytes, namespace:Optional[bytes]=None, timeout: Optional[int | float]=None
                                 ) -> Future[bool]: ...

    #############################################################
    # NodeInfo methods
    #############################################################
    def check_alive(
        self, node_ids: List[NodeID], timeout: Optional[int | float] = None
    ) -> List[bool]: ...

    def async_check_alive(
        self, node_ids: List[NodeID], timeout: Optional[int | float] = None
    ) -> Future[List[bool]]: ...

    def drain_nodes(
        self, node_ids: Sequence[bytes], timeout: Optional[int | float] = None
    ) -> List[bytes]:
        """returns a list of node_ids that are successfully drained."""
        ...

    def get_all_node_info(
        self, timeout: Optional[int | float] = None,
        state_filter: Optional[int] = None,
    ) -> Dict[NodeID, gcs_pb2.GcsNodeInfo]: ...

    def async_get_all_node_info(
        self, node_id: Optional[NodeID] = None, timeout: Optional[int | float] = None
    ) -> Future[Dict[NodeID, gcs_pb2.GcsNodeInfo]]: ...

    #############################################################
    # NodeResources methods
    #############################################################
    def get_all_resource_usage(
        self, timeout: Optional[int | float] = None
    ) -> GetAllResourceUsageReply: ...

    #############################################################
    # Actor methods
    #############################################################

    def async_get_all_actor_info(
        self,
        actor_id: Optional[ActorID] = None,
        job_id: Optional[JobID] = None,
        actor_state_name: Optional[str] = None,
        timeout: Optional[int | float] = None
    ) -> Future[Dict[ActorID, gcs_pb2.ActorTableData]]: ...


    def async_kill_actor(
        self, actor_id: ActorID, force_kill:bool, no_restart:bool,
        timeout: Optional[int | float] = None
    ) -> Future[None]:
        """
        On success: returns None.
        On failure: raises an exception.
        """
        ...

    #############################################################
    # Job methods
    #############################################################

    def get_all_job_info(
        self, *, job_or_submission_id: Optional[str] = None,
        skip_submission_job_info_field: bool = False,
        skip_is_running_tasks_field: bool = False,
        timeout: Optional[int | float] = None
    ) -> Dict[JobID, gcs_pb2.JobTableData]: ...

    def async_get_all_job_info(
        self, *, job_or_submission_id: Optional[str] = None,
        skip_submission_job_info_field: bool = False,
        skip_is_running_tasks_field: bool = False,
        timeout: Optional[int | float] = None
    ) -> Future[Dict[JobID, gcs_pb2.JobTableData]]: ...


    #############################################################
    # Runtime Env methods
    #############################################################
    def pin_runtime_env_uri(self, uri:str, expiration_s:int, timeout:Optional[int | float]=None)->None: ...

    #############################################################
    # Autoscaler methods
    #############################################################
    def request_cluster_resource_constraint(
            self,
            bundles: List[Dict[bytes, float]],
            count_array: List[int],
            timeout_s:Optional[int | float]=None)->None: ...

    def get_cluster_resource_state(
            self,
            timeout_s:Optional[int | float]=None)->bytes: ...

    def get_cluster_status(
            self,
            timeout_s:Optional[int | float]=None)->bytes: ...

    def async_get_cluster_status(
        self,
        timeout_s:Optional[int | float]=None
    ) -> Future[autoscaler_pb2.GetClusterStatusReply]: ...

    def report_autoscaling_state(
        self,
        serialzied_state: bytes,
        timeout_s:Optional[int | float]=None
    )->None:
        """Report autoscaling state to GCS"""
        ...

    def drain_node(
            self,
            node_id: bytes,
            reason: int,
            reason_message: bytes,
            deadline_timestamp_ms: int)->tuple[bool,str]:
        """Send the DrainNode request to GCS.

        This is only for testing.
        """
        ...


    #############################################################
    # Publisher methods
    #############################################################

    def publish_error(self, key_id: bytes, error_type: str, message: str,
                      job_id: Optional[JobID] = None, timeout:Optional[int|float] = None)->None: ...

    def publish_logs(self, log_json: dict, timeout:Optional[int|float] = None)-> None: ...

    def async_publish_node_resource_usage(
            self, key_id: str, node_resource_usage_json: str) -> Future[None]: ...

    def report_cluster_config(
                self,
                serialized_cluster_config: bytes)->None:
        """Report cluster config to GCS"""
        ...

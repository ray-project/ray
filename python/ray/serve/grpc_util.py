from typing import Any, Dict, List, Optional, Tuple

import grpc

from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class RayServegRPCContext:
    """Context manager to set and get gRPC context.

    This class implements most of the methods from ServicerContext
    (see: https://grpc.github.io/grpc/python/grpc.html#grpc.ServicerContext). It's
    serializable and can be passed with the request to be used on the deployment.
    """

    def __init__(self, grpc_context: grpc._cython.cygrpc._ServicerContext):
        self._auth_context = grpc_context.auth_context()
        self._code = grpc_context.code()
        self._details = grpc_context.details()
        self._invocation_metadata = [
            (key, value) for key, value in grpc_context.invocation_metadata()
        ]
        self._peer = grpc_context.peer()
        self._peer_identities = grpc_context.peer_identities()
        self._peer_identity_key = grpc_context.peer_identity_key()
        self._trailing_metadata = [
            (key, value) for key, value in grpc_context.trailing_metadata()
        ]
        self._compression = None

    def auth_context(self) -> Dict[str, Any]:
        return self._auth_context

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def invocation_metadata(self) -> List[Tuple[str, str]]:
        return self._invocation_metadata

    def peer(self) -> str:
        return self._peer

    def peer_identities(self) -> Optional[bytes]:
        return self._peer_identities

    def peer_identity_key(self) -> Optional[str]:
        return self._peer_identity_key

    def trailing_metadata(self) -> List[Tuple[str, str]]:
        return self._trailing_metadata

    def set_code(self, code: grpc.StatusCode):
        self._code = code

    def set_compression(self, compression: grpc.Compression):
        self._compression = compression

    def set_details(self, details: str):
        self._details = details

    def _request_id_metadata(self) -> List[Tuple[str, str]]:
        # Request id metadata should be carried over to the trailing metadata and passed
        # back to the request client. This function helps pick it out if it exists.
        for key, value in self._trailing_metadata:
            if key == "request_id":
                return [(key, value)]
        return []

    def set_trailing_metadata(self, trailing_metadata: List[Tuple[str, str]]):
        self._trailing_metadata = self._request_id_metadata() + trailing_metadata

    def set_on_grpc_context(self, grpc_context: grpc._cython.cygrpc._ServicerContext):
        if self._code:
            grpc_context.set_code(self._code)

        if self._compression:
            grpc_context.set_compression(self._compression)

        if self._details:
            grpc_context.set_details(self._details)

        if self._trailing_metadata:
            grpc_context.set_trailing_metadata(self._trailing_metadata)

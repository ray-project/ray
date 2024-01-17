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
        """Gets the auth context for the call.

        Returns:
          A map of strings to an iterable of bytes for each auth property.
        """
        return self._auth_context

    def code(self) -> grpc.StatusCode:
        """Accesses the value to be used as status code upon RPC completion.

        Returns:
          The StatusCode value for the RPC.
        """
        return self._code

    def details(self) -> str:
        """Accesses the value to be used as detail string upon RPC completion.

        Returns:
          The details string of the RPC.
        """
        return self._details

    def invocation_metadata(self) -> List[Tuple[str, str]]:
        """Accesses the metadata sent by the client.

        Returns:
          The invocation :term:`metadata`.
        """
        return self._invocation_metadata

    def peer(self) -> str:
        """Identifies the peer that invoked the RPC being serviced.

        Returns:
          A string identifying the peer that invoked the RPC being serviced.
          The string format is determined by gRPC runtime.
        """
        return self._peer

    def peer_identities(self) -> Optional[bytes]:
        """Gets one or more peer identity(s).

        Equivalent to
        servicer_context.auth_context().get(servicer_context.peer_identity_key())

        Returns:
          An iterable of the identities, or None if the call is not
          authenticated. Each identity is returned as a raw bytes type.
        """
        return self._peer_identities

    def peer_identity_key(self) -> Optional[str]:
        """The auth property used to identify the peer.

        For example, "x509_common_name" or "x509_subject_alternative_name" are
        used to identify an SSL peer.

        Returns:
          The auth property (string) that indicates the
          peer identity, or None if the call is not authenticated.
        """
        return self._peer_identity_key

    def trailing_metadata(self) -> List[Tuple[str, str]]:
        return self._trailing_metadata

    def set_code(self, code: grpc.StatusCode):
        """Sets the value to be used as status code upon RPC completion.

        This method need not be called by method implementations if they wish
        the gRPC runtime to determine the status code of the RPC.

        Args:
          code: A StatusCode object to be sent to the client.
        """
        self._code = code

    def set_compression(self, compression: grpc.Compression):
        """Set the compression algorithm to be used for the entire call.

        Args:
          compression: An element of grpc.compression, e.g.
            grpc.compression.Gzip.
        """
        self._compression = compression

    def set_details(self, details: str):
        """Sets the value to be used as detail string upon RPC completion.

        This method need not be called by method implementations if they have
        no details to transmit.

        Args:
          details: A UTF-8-encodable string to be sent to the client upon
          termination of the RPC.
        """
        self._details = details

    def _request_id_metadata(self) -> List[Tuple[str, str]]:
        # Request id metadata should be carried over to the trailing metadata and passed
        # back to the request client. This function helps pick it out if it exists.
        for key, value in self._trailing_metadata:
            if key == "request_id":
                return [(key, value)]
        return []

    def set_trailing_metadata(self, trailing_metadata: List[Tuple[str, str]]):
        """Sets the trailing metadata for the RPC.

        Sets the trailing metadata to be sent upon completion of the RPC.

        If this method is invoked multiple times throughout the lifetime of an
        RPC, the value supplied in the final invocation + request id will be the value
        sent over the wire.

        This method need not be called by implementations if they have no
        metadata to add to what the gRPC runtime will transmit.

        Args:
          trailing_metadata: The trailing :term:`metadata`.
        """
        self._trailing_metadata = self._request_id_metadata() + trailing_metadata

    def _set_on_grpc_context(self, grpc_context: grpc._cython.cygrpc._ServicerContext):
        """Serve's internal method to set attributes on the gRPC context."""
        if self._code:
            grpc_context.set_code(self._code)

        if self._compression:
            grpc_context.set_compression(self._compression)

        if self._details:
            grpc_context.set_details(self._details)

        if self._trailing_metadata:
            grpc_context.set_trailing_metadata(self._trailing_metadata)

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from ray.includes.rpc_token_authentication cimport (
    CAuthenticationMode,
    GetAuthenticationMode,
    IsK8sTokenAuthEnabled,
    CAuthenticationToken,
    CAuthenticationTokenLoader,
    CAuthenticationTokenValidator,
    CTokenLoadResult,
)
from ray._private.authentication.authentication_constants import AUTHORIZATION_HEADER_NAME
import logging

logger = logging.getLogger(__name__)


# Authentication mode enum exposed to Python
class AuthenticationMode:
    DISABLED = CAuthenticationMode.DISABLED
    TOKEN = CAuthenticationMode.TOKEN


def get_authentication_mode():
    """Get the current authentication mode.

    Returns:
        AuthenticationMode enum value (DISABLED or TOKEN)
    """
    return GetAuthenticationMode()


def is_k8s_token_auth_enabled():
    """Returns whether Kubernetes token authentication is enabled.

    Returns:
        bool: True if Kubernetes token auth is enabled, false otherwise.
    """
    return IsK8sTokenAuthEnabled()


def validate_authentication_token(provided_metadata: str) -> bool:
    """Validate provided authentication token.

    For TOKEN mode, compares against the expected token.
    If K8S auth is enabled, validates against the Kubernetes API.

    Args:
        provided_metadata: Full authorization header value (e.g., "Bearer <token>")

    Returns:
        bool: True if token is valid, False otherwise
    """
    cdef shared_ptr[const CAuthenticationToken] expected_ptr

    if get_authentication_mode() == CAuthenticationMode.TOKEN and not is_k8s_token_auth_enabled():
        expected_ptr = CAuthenticationTokenLoader.instance().GetToken(False)
        if not expected_ptr:
            return False

    return CAuthenticationTokenValidator.instance().ValidateToken(
        expected_ptr, provided_metadata.encode())


class AuthenticationTokenLoader:
    """Python wrapper for C++ AuthenticationTokenLoader singleton."""

    @staticmethod
    def instance():
        """Get the singleton instance (returns a wrapper for convenience)."""
        return AuthenticationTokenLoader()

    def has_token(self, ignore_auth_mode=False):
        """Check if an authentication token exists without crashing.

        Args:
            ignore_auth_mode: If True, bypass auth mode check and attempt to load token
                            regardless of RAY_AUTH_MODE setting.

        Returns:
            bool: True if a token exists, False otherwise

        Raises:
            AuthenticationError: If any issues loading the token
        """
        cdef CTokenLoadResult result
        cdef c_bool c_ignore_auth_mode = ignore_auth_mode

        with nogil:
            result = CAuthenticationTokenLoader.instance().TryLoadToken(c_ignore_auth_mode)

        if result.hasError():
            from ray.exceptions import AuthenticationError
            raise AuthenticationError(result.error_message.decode('utf-8'))

        if not result.token.has_value() or result.token.value().empty():
            return False
        return True

    def reset_cache(self):
        """Reset the C++ authentication token cache.

        This forces the token loader to reload the token from environment
        variables or files on the next request.
        """
        CAuthenticationTokenLoader.instance().ResetCache()

    def get_token_for_http_header(self, ignore_auth_mode=False) -> dict:
        """Get authentication token as a dictionary for HTTP headers.

        This method loads the token from C++ AuthenticationTokenLoader and returns it
        as a dictionary that can be merged with existing headers. It returns an empty
        dictionary if:
        - A token does not exist
        - The token is empty

        Args:
            ignore_auth_mode: If True, bypass auth mode check and attempt to load token
                            regardless of RAY_AUTH_MODE setting.

        Returns:
            dict: Empty dict or {"authorization": "Bearer <token>"}
        """
        if not self.has_token(ignore_auth_mode):
            return {}

        # Get the token from C++ layer (returns shared_ptr)
        cdef shared_ptr[const CAuthenticationToken] token_ptr = \
            CAuthenticationTokenLoader.instance().GetToken(ignore_auth_mode)

        if not token_ptr or token_ptr.get().empty():
            return {}

        return {AUTHORIZATION_HEADER_NAME: token_ptr.get().ToAuthorizationHeaderValue().decode('utf-8')}

    def get_raw_token(self, ignore_auth_mode=False) -> str:
        """Get the raw authentication token value.

        Args:
            ignore_auth_mode: If True, bypass auth mode check and attempt to load token
                            regardless of RAY_AUTH_MODE setting.

        Returns:
            str: The raw token string, or empty string if no token exists
        """
        if not self.has_token(ignore_auth_mode):
            return ""

        # Get the token from C++ layer (returns shared_ptr)
        cdef shared_ptr[const CAuthenticationToken] token_ptr = \
            CAuthenticationTokenLoader.instance().GetToken(ignore_auth_mode)

        if not token_ptr or token_ptr.get().empty():
            return ""

        return token_ptr.get().GetRawValue().decode('utf-8')

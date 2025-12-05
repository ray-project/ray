from libcpp cimport bool as c_bool
from ray.includes.rpc_token_authentication cimport (
    CAuthenticationMode,
    GetAuthenticationMode,
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
    K8S = CAuthenticationMode.K8S


def get_authentication_mode():
    """Get the current authentication mode.

    Returns:
        AuthenticationMode enum value (DISABLED or TOKEN or K8S)
    """
    return GetAuthenticationMode()


def validate_authentication_token(provided_token: str) -> bool:
    """Validate provided authentication token.

    For TOKEN mode, compares against the expected token.
    For K8S mode, validates against the Kubernetes API.

    Args:
        provided_token: Full authorization header value (e.g., "Bearer <token>")

    Returns:
        bool: True if token is valid, False otherwise
    """
    cdef optional[CAuthenticationToken] expected_opt
    cdef CAuthenticationToken provided

    if get_authentication_mode() == CAuthenticationMode.TOKEN:
        expected_opt = CAuthenticationTokenLoader.instance().GetToken(False)
        if not expected_opt.has_value():
            return False

    # Parse provided token from Bearer format
    provided = CAuthenticationToken.FromMetadata(provided_token.encode())

    if provided.empty():
        return False

    return CAuthenticationTokenValidator.instance().ValidateToken(expected_opt, provided)


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

        # Get the token from C++ layer
        cdef optional[CAuthenticationToken] token_opt = CAuthenticationTokenLoader.instance().GetToken(ignore_auth_mode)

        if not token_opt.has_value() or token_opt.value().empty():
            return {}

        return {AUTHORIZATION_HEADER_NAME: token_opt.value().ToAuthorizationHeaderValue().decode('utf-8')}

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

        # Get the token from C++ layer
        cdef optional[CAuthenticationToken] token_opt = CAuthenticationTokenLoader.instance().GetToken(ignore_auth_mode)

        if not token_opt.has_value() or token_opt.value().empty():
            return ""

        return token_opt.value().GetRawValue().decode('utf-8')

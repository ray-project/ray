from ray.includes.rpc_token_authentication cimport (
    CAuthenticationMode,
    GetAuthenticationMode,
    CAuthenticationToken,
    CAuthenticationTokenLoader,
    CAuthenticationTokenValidator,
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
        expected_opt = CAuthenticationTokenLoader.instance().GetToken()
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

    def has_token(self):
        """Check if an authentication token exists without crashing.

        Returns:
            bool: True if a token exists, False otherwise
        """
        return CAuthenticationTokenLoader.instance().HasToken()

    def reset_cache(self):
        """Reset the C++ authentication token cache.

        This forces the token loader to reload the token from environment
        variables or files on the next request.
        """
        CAuthenticationTokenLoader.instance().ResetCache()

    def get_token_for_http_header(self) -> dict:
        """Get authentication token as a dictionary for HTTP headers.

        This method loads the token from C++ AuthenticationTokenLoader and returns it
        as a dictionary that can be merged with existing headers. It returns an empty
        dictionary if:
        - A token does not exist
        - The token is empty

        Returns:
            dict: Empty dict or {"authorization": "Bearer <token>"}
        """
        if not self.has_token():
            return {}

        # Get the token from C++ layer
        cdef optional[CAuthenticationToken] token_opt = CAuthenticationTokenLoader.instance().GetToken()

        if not token_opt.has_value() or token_opt.value().empty():
            return {}

        return {AUTHORIZATION_HEADER_NAME: token_opt.value().ToAuthorizationHeaderValue().decode('utf-8')}

    def get_raw_token(self) -> str:
        if not self.has_token():
            return ""

        # Get the token from C++ layer
        cdef optional[CAuthenticationToken] token_opt = CAuthenticationTokenLoader.instance().GetToken()

        if not token_opt.has_value() or token_opt.value().empty():
            return ""

        return token_opt.value().GetRawValue().decode('utf-8')

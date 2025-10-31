from ray.includes.rpc_token_authentication cimport (
    CAuthenticationMode,
    GetAuthenticationMode,
    CAuthenticationToken,
    CAuthenticationTokenLoader,
)


# Authentication mode enum exposed to Python
class AuthenticationMode:
    DISABLED = CAuthenticationMode.DISABLED
    TOKEN = CAuthenticationMode.TOKEN

_AUTHORIZATION_HEADER_NAME = "authorization"

def get_authentication_mode():
    """Get the current authentication mode.

    Returns:
        AuthenticationMode enum value (DISABLED or TOKEN)
    """
    return GetAuthenticationMode()


def validate_authentication_token(provided_token: str) -> bool:
    """Validate provided authentication token against expected token.

    Args:
        provided_token: Full authorization header value (e.g., "Bearer <token>")

    Returns:
        bool: True if tokens match, False otherwise
    """
    # Get expected token from loader
    cdef optional[CAuthenticationToken] expected_opt = CAuthenticationTokenLoader.instance().GetToken()

    if not expected_opt.has_value():
        return False

    # Parse provided token from Bearer format
    cdef CAuthenticationToken provided = CAuthenticationToken.FromMetadata(provided_token.encode())

    if provided.empty():
        return False

    # Use constant-time comparison from C++
    return expected_opt.value().Equals(provided)


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

    def set_token_for_http_header(self, headers: dict):
        """Add authentication token to HTTP headers dictionary if token auth is enabled.

        This method loads the token from C++ AuthenticationTokenLoader and adds it
        to the provided headers dictionary as the Authorization header. It only adds
        the token if:
        - Token authentication is enabled
        - A token exists
        - The Authorization header is not already set in the headers

        Args:
            headers: Dictionary of HTTP headers to modify (modified in-place)

        Returns:
            bool: True if token was added, False otherwise
        """
        # Don't override if user explicitly set Authorization header
        if _AUTHORIZATION_HEADER_NAME in headers:
            return False

        # Check if token exists (doesn't crash, returns bool)
        if not self.has_token():
            return False

        # Get the token from C++ layer
        cdef optional[CAuthenticationToken] token_opt = CAuthenticationTokenLoader.instance().GetToken()

        if not token_opt.has_value() or token_opt.value().empty():
            return False

        headers[_AUTHORIZATION_HEADER_NAME] = token_opt.value().ToAuthorizationHeaderValue()
        return True

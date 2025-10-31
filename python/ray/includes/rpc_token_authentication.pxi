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

        return {_AUTHORIZATION_HEADER_NAME: token_opt.value().ToAuthorizationHeaderValue().decode('utf-8')}

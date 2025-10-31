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

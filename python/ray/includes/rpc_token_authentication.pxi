from ray.includes.rpc_token_authentication cimport (
    CAuthenticationMode,
    GetAuthenticationMode,
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

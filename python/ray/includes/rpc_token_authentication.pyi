from typing import Dict

class AuthenticationMode:
    DISABLED: int
    TOKEN: int

def get_authentication_mode() -> int:
    """Get the current authentication mode.

    Returns:
        AuthenticationMode enum value (DISABLED or TOKEN)
    """
    ...


def validate_authentication_token(provided_token: str) -> bool:
    """Validate provided authentication token against expected token.

    Args:
        provided_token: Full authorization header value (e.g., "Bearer <token>")

    Returns:
        bool: True if tokens match, False otherwise
    """
    ...

class AuthenticationTokenLoader:
    """Python wrapper for C++ AuthenticationTokenLoader singleton."""

    @staticmethod
    def instance():
        """Get the singleton instance (returns a wrapper for convenience)."""
        return AuthenticationTokenLoader()

    def has_token(self) -> bool:
        """Check if an authentication token exists without crashing.

        Returns:
            bool: True if a token exists, False otherwise
        """
        ...

    def reset_cache(self):
        """Reset the C++ authentication token cache.

        This forces the token loader to reload the token from environment
        variables or files on the next request.
        """
        ...

    def get_token_for_http_header(self) -> Dict[str,str]:
        """Get authentication token as a dictionary for HTTP headers.

        This method loads the token from C++ AuthenticationTokenLoader and returns it
        as a dictionary that can be merged with existing headers. It returns an empty
        dictionary if:
        - A token does not exist
        - The token is empty

        Returns:
            dict: Empty dict or {"authorization": "Bearer <token>"}
        """
        ...

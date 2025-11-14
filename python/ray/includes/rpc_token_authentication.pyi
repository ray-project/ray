from typing import Dict

class AuthenticationMode:
    DISABLED: int
    TOKEN: int
    K8S: int

def get_authentication_mode() -> int:
    """Get the current authentication mode.

    Returns:
        AuthenticationMode enum value (DISABLED or TOKEN or K8S)
    """
    ...


def validate_authentication_token(provided_token: str) -> bool:
    """Validate provided authentication token.

    For TOKEN mode, compares against the expected token.
    For K8S mode, validates against the Kubernetes API.

    Args:
        provided_token: Full authorization header value (e.g., "Bearer <token>")

    Returns:
        bool: True if token is valid, False otherwise
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

    def get_raw_token(self) -> str: ...

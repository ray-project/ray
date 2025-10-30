# Token setup instructions (used in multiple contexts)
TOKEN_SETUP_INSTRUCTIONS = """Please provide an authentication token using one of these methods:
  1. Set the RAY_AUTH_TOKEN environment variable
  2. Set the RAY_AUTH_TOKEN_PATH environment variable (pointing to a token file)
  3. Create a token file at the default location: ~/.ray/auth_token"""

# When token auth is enabled but no token is found anywhere
TOKEN_AUTH_ENABLED_BUT_NO_TOKEN_FOUND_ERROR_MESSAGE = (
    "Token authentication is enabled but no authentication token was found. "
    + TOKEN_SETUP_INSTRUCTIONS
)

# When HTTP request fails with 401 (Unauthorized - missing token)
HTTP_REQUEST_MISSING_TOKEN_ERROR_MESSAGE = (
    "The Ray cluster requires authentication, but no token was provided.\n\n"
    + TOKEN_SETUP_INSTRUCTIONS
)

# When HTTP request fails with 403 (Forbidden - invalid token)
HTTP_REQUEST_INVALID_TOKEN_ERROR_MESSAGE = (
    "The authentication token you provided is invalid or incorrect.\n\n"
    + TOKEN_SETUP_INSTRUCTIONS
)

# Authentication error messages
TOKEN_AUTH_ENABLED_BUT_NO_TOKEN_FOUND_ERROR_MESSAGE = (
    "Token authentication is enabled but no authentication token was found"
)

TOKEN_INVALID_ERROR_MESSAGE = "Token authentication is enabled but the authentication token is invalid or incorrect."  # noqa: E501

# HTTP header and cookie constants
AUTHORIZATION_HEADER_NAME = "authorization"
AUTHORIZATION_BEARER_PREFIX = "Bearer "
RAY_AUTHORIZATION_HEADER_NAME = "x-ray-authorization"

AUTHENTICATION_TOKEN_COOKIE_NAME = "ray-authentication-token"
AUTHENTICATION_TOKEN_COOKIE_MAX_AGE = 30 * 24 * 60 * 60  # 30 days

# Authentication error messages
TOKEN_AUTH_FIX_MESSAGE = (
    "To connect to a cluster with token authentication, run `ray get-auth-token` "
    "on the head node and copy the token to ~/.ray/auth_token on this machine, "
    "or to a file that RAY_AUTH_TOKEN_PATH points to. If the cluster was started "
    "with RAY_AUTH_MODE=disabled, set RAY_AUTH_MODE=disabled here too. See "
    "https://docs.ray.io/en/latest/ray-security/token-auth.html for more information."
)

# Keep in sync with kNoTokenErrorMessage in
# src/ray/rpc/authentication/authentication_token_loader.cc.
TOKEN_AUTH_ENABLED_BUT_NO_TOKEN_FOUND_ERROR_MESSAGE = (
    "Token authentication is enabled but no authentication token was found. "
    + TOKEN_AUTH_FIX_MESSAGE
)

# Keep in sync with kAuthModeMismatchErrorMessage in src/ray/common/constants.h.
AUTH_MODE_MISMATCH_ERROR_MESSAGE = (
    "AuthModeMismatch: cluster has RAY_AUTH_MODE=disabled; set it on the client too"
)

TOKEN_INVALID_ERROR_MESSAGE = "Token authentication is enabled but the authentication token is invalid or incorrect."  # noqa: E501

# HTTP header and cookie constants
AUTHORIZATION_HEADER_NAME = "authorization"
AUTHORIZATION_BEARER_PREFIX = "Bearer "
RAY_AUTHORIZATION_HEADER_NAME = "x-ray-authorization"

AUTHENTICATION_TOKEN_COOKIE_NAME = "ray-authentication-token"
AUTHENTICATION_TOKEN_COOKIE_MAX_AGE = 30 * 24 * 60 * 60  # 30 days

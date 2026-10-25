# Authentication error messages
TOKEN_AUTH_FIX_MESSAGE = (
    "To connect to a cluster with token authentication, run `ray get-auth-token` "
    "on the head node and copy the token to ~/.ray/auth_token on this machine, "
    "or to a file that RAY_AUTH_TOKEN_PATH points to. If the cluster was started "
    "with RAY_AUTH_MODE=disabled, set RAY_AUTH_MODE=disabled here too. See "
    "https://docs.ray.io/en/latest/ray-security/token-auth.html for more information."
)

# For a client with no token and for a token sent to a cluster with auth disabled.
# Keep in sync with kTokenAuthErrorMessage in src/ray/common/constants.h.
TOKEN_AUTH_ERROR_MESSAGE = (
    "Token authentication is enabled on this client, but it has no token or the "
    "cluster has token authentication disabled. " + TOKEN_AUTH_FIX_MESSAGE
)

# Keep in sync with kK8sTokenAuthRequiresTokenModeErrorMessage in
# src/ray/rpc/authentication/authentication_token_loader.cc.
K8S_TOKEN_AUTH_REQUIRES_TOKEN_MODE_ERROR_MESSAGE = (
    "Kubernetes token authentication (ENABLE_K8S_TOKEN_AUTH) requires "
    "RAY_AUTH_MODE=token. Set RAY_AUTH_MODE=token, or turn off "
    "ENABLE_K8S_TOKEN_AUTH to run with auth disabled."
)

TOKEN_INVALID_ERROR_MESSAGE = "Token authentication is enabled but the authentication token is invalid or incorrect."  # noqa: E501

# HTTP header and cookie constants
AUTHORIZATION_HEADER_NAME = "authorization"
AUTHORIZATION_BEARER_PREFIX = "Bearer "
RAY_AUTHORIZATION_HEADER_NAME = "x-ray-authorization"

AUTHENTICATION_TOKEN_COOKIE_NAME = "ray-authentication-token"
AUTHENTICATION_TOKEN_COOKIE_MAX_AGE = 30 * 24 * 60 * 60  # 30 days

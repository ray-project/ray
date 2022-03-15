# Reserved constant used as key in other_args_to_resolve to configure if we
# return sync or async handle of a deployment.
# True -> RayServeSyncHandle
# False -> RayServeHandle
USE_SYNC_HANDLE_KEY = "__use_sync_handle__"
# Reserved key to pass and resolve deployment configs from Deployment class
DEPLOYMENT_CONFIG_KEY = "__deployment_config__"

import ray

from ray._private.client_mode_hook import client_mode_hook


@client_mode_hook(auto_init=False)
def get_current_runtime_env():
    """Get the runtime env of the current job/worker.

    If this API is called in driver or ray client, returns the job level runtime env.
    If this API is called in workers/actors, returns the worker level runtime env.

    Returns:
        A dict of the current runtime env

    To merge from the parent runtime env in some specific cases, you can get the parent
    runtime env by this API and modify it by yourself.

    Example:

    >>> # Inherit parent runtime env, except `env_vars`
    >>> Actor.options(runtime_env=ray.get_current_runtime_env().update(
        {"env_vars": {"A": "a", "B": "b"}}))
    """

    return dict(ray.get_runtime_context().runtime_env)

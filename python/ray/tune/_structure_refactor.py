import warnings

from ray.util import log_once


def warn_structure_refactor(old_module: str, new_module: str):
    old_module = old_module.replace(".py", "")
    if log_once(f"tune:structure:refactor:{old_module}"):
        warnings.warn(
            f"The module `{old_module}` has been moved to `{new_module}` and the old "
            f"location will be deprecated soon. Please adjust your imports to point "
            f"to the new location. Example: Do a global search and "
            f"replace `{old_module}` with `{new_module}`.",
            DeprecationWarning,
        )

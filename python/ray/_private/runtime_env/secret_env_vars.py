import logging
from typing import List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.runtime_env import decode_secret_env_vars

default_logger = logging.getLogger(__name__)

_PLUGIN_NAME = "secret_env_vars"


class SecretEnvVarsPlugin(RuntimeEnvPlugin):
    @staticmethod
    def validate(runtime_env_dict: dict) -> None:
        """Validate user entry for this plugin.

        Args:
            runtime_env_dict: the user-supplied runtime environment dict.

        Raises:
            ValueError: if the validation fails.
        """

        if _PLUGIN_NAME not in runtime_env_dict.keys():
            return

        secret_env_vars = runtime_env_dict[_PLUGIN_NAME]
        # Validate the secret_env_vars is ad dict.
        if not isinstance(secret_env_vars, dict):
            raise ValueError(
                f"secret_env_vars must be a dict, got {type(secret_env_vars)}."
            )
        # Validate each key and value is str. Collect all violations and raise in the end.
        violations = []
        for key, value in secret_env_vars.items():
            if not isinstance(key, str):
                violations.append(f"key {key} is of type {type(key)}.")
            if not isinstance(value, str):
                violations.append(f"value {value} is of type {type(value)}.")
        if violations:
            raise ValueError(
                "All keys and values in secret_env_vars must be of type str. "
                + " ".join(violations)
            )

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger  # noqa: F821
    ) -> int:
        """Delete URI and return the number of bytes deleted. No-op for this plugin."""
        return 0

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        return []

    async def create(
        self,
        uri: Optional[str],  # noqa: F821
        runtime_env: dict,  # noqa: F821
        context: RuntimeEnvContext,  # noqa: F821
        logger: logging.Logger = default_logger,  # noqa: F821
    ) -> int:
        """Create URI and return the number of bytes used. No-op for this plugin."""
        return 0

    def modify_context(
        self,
        uris: List[str],  # noqa: F821
        runtime_env_dict: dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,  # noqa: F821
    ):
        """Modify context to change worker startup behavior."""

        # Add secrets to context env vars.
        encoded_secret_env_vars = runtime_env_dict.get(_PLUGIN_NAME, {})
        # The secrets are base64 encoded for safety. We decode them before adding to the context.
        secret_env_vars = decode_secret_env_vars(encoded_secret_env_vars)
        for key, value in secret_env_vars.items():
            context.env_vars[key] = value

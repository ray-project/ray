import unittest

import yaml

from ray.autoscaler._private.providers import (
    _DEFAULT_CONFIGS,
    _NODE_PROVIDERS,
    _PROVIDER_PRETTY_NAMES,
)


class TestProviders(unittest.TestCase):
    def test_node_providers(self):
        for provider_name, provider_cls in _NODE_PROVIDERS.items():
            config = {"module": "ray.autoscaler._private"}

            try:
                provider_cls(config)
            except ImportError as e:
                if f"ray.autoscaler.{provider_name}" in str(e):
                    self.fail(
                        f"Unexpected import error for provider {provider_name}: {e}"
                    )

    def test_provider_pretty_names(self):
        self.assertEqual(
            set(_NODE_PROVIDERS.keys()), set(_PROVIDER_PRETTY_NAMES.keys())
        )

    def test_default_configs(self):
        for config_loader in _DEFAULT_CONFIGS.values():
            config_path = config_loader()
            with open(config_path) as f:
                yaml.safe_load(f)


if __name__ == "__main__":
    unittest.main()

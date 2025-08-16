import sys
from unittest.mock import MagicMock

# This file is automatically loaded by pytest.
# We mock out the azure modules that are dependencies of
# launch_and_verify_cluster.py, so that we can run doctest
# on that file without having azure installed.

MOCK_AZURE = MagicMock()
MOCK_AZURE.keyvault = MagicMock()
MOCK_AZURE.keyvault.secrets = MagicMock()
MOCK_AZURE.identity = MagicMock()
MOCK_AZURE.core = MagicMock()
MOCK_AZURE.core.exceptions = MagicMock()
MOCK_AZURE.mgmt = MagicMock()
MOCK_AZURE.mgmt.compute = MagicMock()
MOCK_AZURE.mgmt.resource = MagicMock()
MOCK_AZURE.mgmt.resource.resources = MagicMock()
MOCK_AZURE.mgmt.resource.resources.models = MagicMock()
MOCK_AZURE.mgmt.network = MagicMock()
MOCK_AZURE.common = MagicMock()
MOCK_AZURE.common.credentials = MagicMock()

sys.modules["azure"] = MOCK_AZURE
sys.modules["azure.keyvault"] = MOCK_AZURE.keyvault
sys.modules["azure.keyvault.secrets"] = MOCK_AZURE.keyvault.secrets
sys.modules["azure.identity"] = MOCK_AZURE.identity
sys.modules["azure.core"] = MOCK_AZURE.core
sys.modules["azure.core.exceptions"] = MOCK_AZURE.core.exceptions
sys.modules["azure.mgmt"] = MOCK_AZURE.mgmt
sys.modules["azure.mgmt.compute"] = MOCK_AZURE.mgmt.compute
sys.modules["azure.mgmt.resource"] = MOCK_AZURE.mgmt.resource
sys.modules["azure.mgmt.resource.resources"] = MOCK_AZURE.mgmt.resource.resources
sys.modules[
    "azure.mgmt.resource.resources.models"
] = MOCK_AZURE.mgmt.resource.resources.models
sys.modules["azure.mgmt.network"] = MOCK_AZURE.mgmt.network
sys.modules["azure.common"] = MOCK_AZURE.common
sys.modules["azure.common.credentials"] = MOCK_AZURE.common.credentials

MOCK_MSRESTAZURE = MagicMock()
MOCK_MSRESTAZURE.azure = MagicMock()
MOCK_MSRESTAZURE.azure.exceptions = MagicMock()

sys.modules["msrestazure"] = MOCK_MSRESTAZURE
sys.modules["msrestazure.azure"] = MOCK_MSRESTAZURE.azure
sys.modules["msrestazure.azure.exceptions"] = MOCK_MSRESTAZURE.azure.exceptions

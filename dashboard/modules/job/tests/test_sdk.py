import pytest
from typing import Dict, Optional, Tuple
from unittest.mock import Mock, patch

from ray.dashboard.modules.dashboard_sdk import parse_cluster_info


@pytest.mark.parametrize(
    "address_param",
    [
        ("ray://1.2.3.4:10001", "ray", "1.2.3.4:10001"),
        ("other_module://", "other_module", ""),
        ("other_module://address", "other_module", "address"),
    ],
)
@pytest.mark.parametrize("create_cluster_if_needed", [True, False])
@pytest.mark.parametrize("cookies", [None, {"test_cookie_key": "test_cookie_val"}])
@pytest.mark.parametrize("metadata", [None, {"test_metadata_key": "test_metadata_val"}])
@pytest.mark.parametrize("headers", [None, {"test_headers_key": "test_headers_val"}])
def test_parse_cluster_info(
    address_param: Tuple[str, str, str],
    create_cluster_if_needed: bool,
    cookies: Optional[Dict[str, str]],
    metadata: Optional[Dict[str, str]],
    headers: Optional[Dict[str, str]],
):
    """
    Test ray.dashboard.modules.dashboard_sdk.parse_cluster_info for different
    format of addresses.
    """
    mock_get_job_submission_client_cluster = Mock(return_value="Ray ClusterInfo")
    mock_module = Mock()
    mock_module.get_job_submission_client_cluster_info = Mock(
        return_value="Other module ClusterInfo"
    )
    mock_import_module = Mock(return_value=mock_module)

    address, module_string, inner_address = address_param

    with patch.multiple(
        "ray.dashboard.modules.dashboard_sdk",
        get_job_submission_client_cluster_info=mock_get_job_submission_client_cluster,
    ), patch.multiple("importlib", import_module=mock_import_module):
        if module_string == "ray":
            assert (
                parse_cluster_info(
                    address,
                    create_cluster_if_needed=create_cluster_if_needed,
                    cookies=cookies,
                    metadata=metadata,
                    headers=headers,
                )
                == "Ray ClusterInfo"
            )
            mock_get_job_submission_client_cluster.assert_called_once_with(
                inner_address,
                create_cluster_if_needed=create_cluster_if_needed,
                cookies=cookies,
                metadata=metadata,
                headers=headers,
            )
        elif module_string == "other_module":
            assert (
                parse_cluster_info(
                    address,
                    create_cluster_if_needed=create_cluster_if_needed,
                    cookies=cookies,
                    metadata=metadata,
                    headers=headers,
                )
                == "Other module ClusterInfo"
            )
            mock_import_module.assert_called_once_with(module_string)
            mock_module.get_job_submission_client_cluster_info.assert_called_once_with(
                inner_address,
                create_cluster_if_needed=create_cluster_if_needed,
                cookies=cookies,
                metadata=metadata,
                headers=headers,
            )

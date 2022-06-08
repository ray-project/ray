import click
import logging
import json
import yaml

from enum import Enum, unique
from typing import Union, List

import ray
import ray.ray_constants as ray_constants
import ray._private.services as services

from ray.experimental.state.api import StateApiClient
from ray.experimental.state.common import (
    DEFAULT_LIMIT,
    DEFAULT_RPC_TIMEOUT,
    ListApiOptions,
    StateResource,
)
from ray._private.gcs_utils import use_gcs_for_bootstrap
from ray._private.gcs_utils import GcsClient

logger = logging.getLogger(__name__)


@unique
class AvailableFormat(Enum):
    DEFAULT = "default"
    JSON = "json"
    YAML = "yaml"
    TABLE = "table"


def _get_available_formats() -> List[str]:
    """Return the available formats in a list of string"""
    return [format_enum.value for format_enum in AvailableFormat]


def _get_available_resources() -> List[str]:
    """Return the available resources in a list of string"""
    # All resource names use '_' rather than '-'. But users options have '-'
    return [e.value.replace("_", "-") for e in StateResource]


def get_state_api_output_to_print(
    state_data: Union[dict, list], *, format: AvailableFormat = AvailableFormat.DEFAULT
):
    if len(state_data) == 0:
        return "No resource in the cluster"

    # Default is yaml.
    if format == AvailableFormat.DEFAULT:
        return yaml.dump(state_data, indent=4, explicit_start=True)
    if format == AvailableFormat.YAML:
        return yaml.dump(state_data, indent=4, explicit_start=True)
    elif format == AvailableFormat.JSON:
        return json.dumps(state_data)
    elif format == AvailableFormat.TABLE:
        raise NotImplementedError("Table formatter is not implemented yet.")
    else:
        raise ValueError(
            f"Unexpected format: {format}. "
            f"Supported formatting: {_get_available_formats()}"
        )


def _should_explain(format: AvailableFormat):
    # If the format is json or yaml, it should not print stats because
    # users don't want additional strings.
    return format == AvailableFormat.DEFAULT or format == AvailableFormat.TABLE


def _get_state_api_server_address() -> str:
    address = services.canonicalize_bootstrap_address(None)
    gcs_client = GcsClient(address=address, nums_reconnect_retry=0)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    api_server_url = ray._private.utils.internal_kv_get_with_retry(
        gcs_client,
        ray_constants.DASHBOARD_ADDRESS,
        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        num_retries=20,
    )
    api_server_url = api_server_url.decode()
    if api_server_url is None:
        raise ValueError(
            (
                "Couldn't obtain the API server address from GCS. It is likely that "
                "the GCS server is down. Check gcs_server.[out | err] to see if it is "
                "still alive."
            )
        )

    assert use_gcs_for_bootstrap()
    api_server_url = f"http://{api_server_url}"

    return api_server_url


@click.command()
@click.argument(
    "resource",
    type=click.Choice(_get_available_resources()),
)
@click.option(
    "--format", default="default", type=click.Choice(_get_available_formats())
)
@click.option(
    "--timeout",
    default=DEFAULT_RPC_TIMEOUT,
    help=f"Timeout in seconds for the API requests. Default is {DEFAULT_RPC_TIMEOUT}",
)
@click.option(
    "--address",
    default="",
    help=(
        "The address of Ray API server. If not provided, it will be configured "
        "automatically from querying the GCS server."
    ),
)
def list(resource: str, format: str, timeout: float, address: str):
    """
    List RESOURCE used by Ray.

    RESOURCE is the name of the possible resources from `StateResource`,
    i.e. 'jobs', 'actors', 'nodes', ...

    """
    resource = StateResource(resource)
    format = AvailableFormat(format)

    # Get the state API server address from ray if not provided by user
    api_server_address = address if address else _get_state_api_server_address()

    # Create the State API server and put it into context
    logger.debug(f"Create StateApiClient at {api_server_address}...")
    client = StateApiClient(
        api_server_address=api_server_address,
    )

    options = ListApiOptions(
        limit=DEFAULT_LIMIT,  # TODO(rickyyx): should we make this configurable?
        timeout=timeout,
    )

    # If errors occur, exceptions will be thrown. Empty data indicate successful query.
    data = client.list(resource, options=options, _explain=_should_explain(format))

    # Print data to console.
    print(
        get_state_api_output_to_print(
            state_data=data,
            format=format,
        )
    )

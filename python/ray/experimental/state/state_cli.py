import json
import logging
from enum import Enum, unique
from typing import List, Optional, Tuple, Union

import click
import yaml

import ray
import ray._private.ray_constants as ray_constants
import ray._private.services as services
from ray._private.gcs_utils import GcsClient
from ray.experimental.state.api import (
    StateApiClient,
    summarize_actors,
    summarize_objects,
    summarize_tasks,
)
from ray.experimental.state.common import (
    DEFAULT_LIMIT,
    DEFAULT_RPC_TIMEOUT,
    GetApiOptions,
    ListApiOptions,
    PredicateType,
    StateResource,
    SupportedFilterType,
)

logger = logging.getLogger(__name__)


@unique
class AvailableFormat(Enum):
    DEFAULT = "default"
    JSON = "json"
    YAML = "yaml"
    TABLE = "table"


def _parse_filter(filter: str) -> Tuple[str, PredicateType, SupportedFilterType]:
    """Parse the filter string to a tuple of key, preciate, and value."""
    # The function assumes there's going to be no key that includes "="" or "!=".
    # Since key is controlled by us, it should be trivial to keep the invariant.
    predicate = None
    # Tuple of [predicate_start, predicate_end).
    predicate_index = None

    # Find the first predicate match. This logic works because we assume the
    # key doesn't contain = or !=.
    for i in range(len(filter)):
        char = filter[i]
        if char == "=":
            predicate = "="
            predicate_index = (i, i + 1)
            break
        elif char == "!":
            if len(filter) <= i + 1:
                continue

            next_char = filter[i + 1]
            if next_char == "=":
                predicate = "!="
                predicate_index = (i, i + 2)
                break

    if not predicate or not predicate_index:
        raise ValueError(
            f"The format of a given filter {filter} is invalid: "
            "Cannot find the predicate. "
            "Please provide key=val or key!=val format string."
        )

    key, predicate, value = (
        filter[: predicate_index[0]],
        filter[predicate_index[0] : predicate_index[1]],
        filter[predicate_index[1] :],
    )

    assert predicate == "=" or predicate == "!="
    if len(key) == 0 or len(value) == 0:
        raise ValueError(
            f"The format of a given filter {filter} is invalid: "
            f"Cannot identify key {key} or value, {value}. "
            "Please provide key=val or key!=val format string."
        )

    return (key, predicate, value)


def _get_available_formats() -> List[str]:
    """Return the available formats in a list of string"""
    return [format_enum.value for format_enum in AvailableFormat]


def _get_available_resources(
    excluded: Optional[List[StateResource]] = None,
) -> List[str]:
    """Return the available resources in a list of string

    Args:
        excluded: List of resources that should be excluded
    """
    # All resource names use '_' rather than '-'. But users options have '-'
    return [
        e.value.replace("_", "-")
        for e in StateResource
        if excluded is None or e not in excluded
    ]


def get_api_server_url() -> str:
    address = services.canonicalize_bootstrap_address(None)
    gcs_client = GcsClient(address=address, nums_reconnect_retry=0)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    api_server_url = ray._private.utils.internal_kv_get_with_retry(
        gcs_client,
        ray_constants.DASHBOARD_ADDRESS,
        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        num_retries=20,
    )

    if api_server_url is None:
        raise ValueError(
            (
                "Couldn't obtain the API server address from GCS. It is likely that "
                "the GCS server is down. Check gcs_server.[out | err] to see if it is "
                "still alive."
            )
        )

    api_server_url = f"http://{api_server_url.decode()}"
    return api_server_url


def output_with_format(
    state_data: Union[dict, list], format: AvailableFormat = AvailableFormat.DEFAULT
):
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


def format_get_api_output(
    state_data: Union[dict, list],
    id: str,
    format: AvailableFormat = AvailableFormat.DEFAULT,
):
    if len(state_data) == 0:
        return f"Resource with id={id} not found in the cluster."

    return output_with_format(state_data, format)


def format_list_api_output(
    state_data: Union[dict, list], *, format: AvailableFormat = AvailableFormat.DEFAULT
):
    if len(state_data) == 0:
        return "No resource in the cluster"
    return output_with_format(state_data, format)


def _should_explain(format: AvailableFormat):
    # If the format is json or yaml, it should not print stats because
    # users don't want additional strings.
    return format == AvailableFormat.DEFAULT or format == AvailableFormat.TABLE


"""
Common Options for State API commands
"""
timeout_option = click.option(
    "--timeout",
    default=DEFAULT_RPC_TIMEOUT,
    help=f"Timeout in seconds for the API requests. Default is {DEFAULT_RPC_TIMEOUT}",
)
address_option = click.option(
    "--address",
    default="",
    help=(
        "The address of Ray API server. If not provided, it will be configured "
        "automatically from querying the GCS server."
    ),
)


# TODO(rickyyx): Once we have other APIs stablized, we should refactor them to
# reuse some of the options, e.g. `--address`.
# list/get/summary could all go under a single command group for options sharing.
@click.command()
@click.argument(
    "resource",
    # NOTE(rickyyx): We are not allowing query job with id, and runtime envs
    type=click.Choice(
        _get_available_resources(
            excluded=[StateResource.JOBS, StateResource.RUNTIME_ENVS]
        )
    ),
)
@click.argument(
    "id",
    type=str,
)
@address_option
@timeout_option
def get(
    resource: str,
    id: str,
    address: Optional[str],
    timeout: float,
):
    """
    Get RESOURCE by ID.

    RESOURCE is the name of the possible resources from `StateResource`,
    i.e. 'workers', 'actors', 'nodes', ...

    NOTE: We currently DO NOT support get by id for jobs and runtime-envs

    Example:

    ```
    ray get nodes <node-id>
    ray get workers <worker-id>
    ```
    """
    # All resource names use '_' rather than '-'. But users options have '-'
    resource = StateResource(resource.replace("-", "_"))

    # Get the state API server address from ray if not provided by user
    api_server_address = address if address else get_api_server_url()

    # Create the State API server and put it into context
    logger.debug(f"Create StateApiClient at {api_server_address}...")
    client = StateApiClient(
        api_server_address=api_server_address,
    )

    options = GetApiOptions(
        timeout=timeout,
    )

    # If errors occur, exceptions will be thrown.
    data = client.get(
        resource=resource,
        id=id,
        options=options,
        _explain=_should_explain(AvailableFormat.YAML),
    )

    # Print data to console.
    print(
        format_list_api_output(
            state_data=data,
            format=AvailableFormat.YAML,
        )
    )


@click.command()
@click.argument(
    "resource",
    type=click.Choice(_get_available_resources()),
)
@click.option(
    "--format", default="default", type=click.Choice(_get_available_formats())
)
@click.option(
    "-f",
    "--filter",
    help=(
        "A key, predicate, and value to filter the result. "
        "E.g., --filter 'key=value' or --filter 'key!=value'. "
        "You can specify multiple --filter options. In this case all predicates "
        "are concatenated as AND. For example, --filter key=value --filter key2=value "
        "means (key==val) AND (key2==val2)"
    ),
    multiple=True,
)
@click.option(
    "--detail",
    help=(
        "If the flag is set, the output will contain data in more details. "
        "Note that the API could query more sources "
        "to obtain information in a greater detail."
    ),
    is_flag=True,
    default=False,
)
@timeout_option
@address_option
def list(
    resource: str,
    format: str,
    detail: bool,
    filter: List[str],
    timeout: float,
    address: str,
):
    """
    List RESOURCE used by Ray.

    RESOURCE is the name of the possible resources from `StateResource`,
    i.e. 'jobs', 'actors', 'nodes', ...

    """
    # All resource names use '_' rather than '-'. But users options have '-'
    resource = StateResource(resource.replace("-", "_"))
    format = AvailableFormat(format)

    # Get the state API server address from ray if not provided by user
    api_server_address = address if address else get_api_server_url()

    # Create the State API server and put it into context
    logger.debug(f"Create StateApiClient at {api_server_address}...")
    client = StateApiClient(
        api_server_address=api_server_address,
    )

    filter = [_parse_filter(f) for f in filter]

    options = ListApiOptions(
        limit=DEFAULT_LIMIT,  # TODO(rickyyx): parameters discussion to be finalized
        timeout=timeout,
        filters=filter,
        detail=detail,
    )

    # If errors occur, exceptions will be thrown. Empty data indicate successful query.
    data = client.list(resource, options=options, _explain=_should_explain(format))

    # Print data to console.
    print(
        format_list_api_output(
            state_data=data,
            format=format,
        )
    )


@click.group("summary")
@click.pass_context
def summary_state_cli_group(ctx):
    ctx.ensure_object(dict)
    ctx.obj["api_server_url"] = get_api_server_url()


@summary_state_cli_group.command(name="tasks")
@timeout_option
@address_option
@click.pass_context
def task_summary(ctx, timeout: float, address: str):
    address = address or ctx.obj["api_server_url"]
    print(
        output_with_format(
            summarize_tasks(
                address=address,
                timeout=timeout,
                _explain=True,
            ),
            format=AvailableFormat.YAML,
        )
    )


@summary_state_cli_group.command(name="actors")
@timeout_option
@address_option
@click.pass_context
def actor_summary(ctx, timeout: float, address: str):
    address = address or ctx.obj["api_server_url"]
    print(
        output_with_format(
            summarize_actors(
                address=address,
                timeout=timeout,
                _explain=True,
            ),
            format=AvailableFormat.YAML,
        )
    )


@summary_state_cli_group.command(name="objects")
@timeout_option
@address_option
@click.pass_context
def object_summary(ctx, timeout: float, address: str):
    address = address or ctx.obj["api_server_url"]
    print(
        output_with_format(
            summarize_objects(
                address=address,
                timeout=timeout,
                _explain=True,
            ),
            format=AvailableFormat.YAML,
        )
    )

import json
import logging
from datetime import datetime
from enum import Enum, unique
from typing import Dict, List, Optional, Tuple

import click
import yaml

from ray._private.thirdparty.tabulate.tabulate import tabulate
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
    StateSchema,
    SupportedFilterType,
    resource_to_schema,
)
from ray.experimental.state.exception import RayStateApiException
from ray.util.annotations import PublicAPI

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


def get_table_output(state_data: List, schema: StateSchema) -> str:
    """Display the table output.

    The table headers are ordered as the order defined in the dataclass of
    `StateSchema`. For example,

    @dataclass
    class A(StateSchema):
        a: str
        b: str
        c: str

    will create headers
    A B C
    -----

    Args:
        state_data: A list of state data.
        schema: The schema for the corresponding resource.

    Returns:
        The table formatted string.
    """
    time = datetime.now()
    header = "=" * 8 + f" List: {time} " + "=" * 8
    headers = []
    table = []
    cols = schema.list_columns()
    for data in state_data:
        for key, val in data.items():
            if isinstance(val, dict):
                data[key] = yaml.dump(val, indent=2)
        keys = set(data.keys())
        headers = []
        for col in cols:
            if col in keys:
                headers.append(col.upper())
        table.append([data[header.lower()] for header in headers])
    return f"""
{header}
Stats:
------------------------------
Total: {len(state_data)}

Table:
------------------------------
{tabulate(table, headers=headers, showindex=True, tablefmt="plain", floatfmt=".3f")}
"""


def output_with_format(
    state_data: List,
    *,
    schema: Optional[StateSchema],
    format: AvailableFormat = AvailableFormat.DEFAULT,
) -> str:
    if format == AvailableFormat.DEFAULT:
        return get_table_output(state_data, schema)
    if format == AvailableFormat.YAML:
        return yaml.dump(state_data, indent=4, explicit_start=True)
    elif format == AvailableFormat.JSON:
        return json.dumps(state_data)
    elif format == AvailableFormat.TABLE:
        return get_table_output(state_data, schema)
    else:
        raise ValueError(
            f"Unexpected format: {format}. "
            f"Supported formatting: {_get_available_formats()}"
        )


def format_summary_output(state_data: Dict, *, resource: StateResource) -> str:
    if len(state_data) == 0:
        return "No resource in the cluster"

    # Parse the data.
    cluster_data = state_data["cluster"]
    summaries = cluster_data["summary"]
    summary_by = cluster_data["summary_by"]
    del cluster_data["summary_by"]
    del cluster_data["summary"]

    cluster_info_table = yaml.dump(cluster_data, indent=2)

    # Create a table.
    table = []
    headers = []
    for summary in summaries.values():
        # Convert dict to yaml for better formatting.
        for key, val in summary.items():
            if isinstance(val, dict):
                summary[key] = yaml.dump(val, indent=2)

        headers = sorted([key.upper() for key in summary.keys()])
        table.append([summary[header.lower()] for header in headers])

    summary_table = tabulate(
        table, headers=headers, showindex=True, tablefmt="plain", numalign="left"
    )

    time = datetime.now()
    header = "=" * 8 + f" {resource.value.capitalize()} Summary: {time} " + "=" * 8
    return f"""
{header}
Stats:
------------------------------------
{cluster_info_table}

Table (group by {summary_by}):
------------------------------------
{summary_table}
"""


def format_object_summary_output(state_data: Dict) -> str:
    if len(state_data) == 0:
        return "No resource in the cluster"

    # Parse the data.
    cluster_data = state_data["cluster"]
    summaries = cluster_data["summary"]
    summary_by = cluster_data["summary_by"]
    del cluster_data["summary_by"]
    del cluster_data["summary"]

    cluster_info_table = yaml.dump(cluster_data, indent=2)

    # Create a table per callsite.
    tables = []
    for callsite, summary in summaries.items():
        # Convert dict to yaml for better formatting.
        for key, val in summary.items():
            if isinstance(val, dict):
                summary[key] = yaml.dump(val, indent=2)

        table = []
        headers = sorted([key.upper() for key in summary.keys()])
        table.append([summary[header.lower()] for header in headers])
        table_for_callsite = tabulate(
            table, headers=headers, showindex=True, numalign="left"
        )

        # Format callsite. | is a separator for ray callsite.
        formatted_callsite = callsite.replace("|", "\n|")
        tables.append(f"{formatted_callsite}\n{table_for_callsite}")

    time = datetime.now()
    header = "=" * 8 + f" Object Summary: {time} " + "=" * 8
    table_string = "\n\n\n\n".join(tables)
    return f"""
{header}
Stats:
------------------------------------
{cluster_info_table}

Table (group by {summary_by})
------------------------------------
{table_string}
"""


def format_get_api_output(
    state_data: Optional[Dict],
    id: str,
    *,
    schema: StateSchema,
    format: AvailableFormat = AvailableFormat.YAML,
) -> str:
    if not state_data or len(state_data) == 0:
        return f"Resource with id={id} not found in the cluster."

    return output_with_format(state_data, schema=schema, format=format)


def format_list_api_output(
    state_data: List[Dict],
    *,
    schema: StateSchema,
    format: AvailableFormat = AvailableFormat.DEFAULT,
) -> str:
    if len(state_data) == 0:
        return "No resource in the cluster"
    return output_with_format(state_data, schema=schema, format=format)


def _should_explain(format: AvailableFormat) -> bool:
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
    default=None,
    help=(
        "The address of Ray API server. If not provided, it will be configured "
        "automatically from querying the GCS server."
    ),
)


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
@PublicAPI(stability="alpha")
def ray_get(
    resource: str,
    id: str,
    address: Optional[str],
    timeout: float,
):
    """Get a state of a given resource by ID.

    We currently DO NOT support get by id for jobs and runtime-envs

    The output schema is defined at :ref:`State API Schema section. <state-api-schema>`

    For example, the output schema of `ray get tasks <task-id>` is
    :ref:`ray.experimental.state.common.TaskState <state-api-schema-task>`.

    Usage:

        Get an actor with actor id <actor-id>

        ```
        ray get actors <actor-id>
        ```

        Get a placement group information with <placement-group-id>

        ```
        ray get placement-groups <placement-group-id>
        ```

    The API queries one or more components from the cluster to obtain the data.
    The returned state snapshot could be stale, and it is not guaranteed to return
    the live data.

    Args:
        resource: The type of the resource to query.
        id: The id of the resource.

    Raises:
        :ref:`RayStateApiException <state-api-exceptions>`
            if the CLI is failed to query the data.
    """
    # All resource names use '_' rather than '-'. But users options have '-'
    resource = StateResource(resource.replace("-", "_"))

    # Create the State API server and put it into context
    logger.debug(f"Create StateApiClient to ray instance at: {address}...")
    client = StateApiClient(address=address)
    options = GetApiOptions(timeout=timeout)

    # If errors occur, exceptions will be thrown.
    try:
        data = client.get(
            resource=resource,
            id=id,
            options=options,
            _explain=_should_explain(AvailableFormat.YAML),
        )
    except RayStateApiException as e:
        raise click.UsageError(str(e))

    # Print data to console.
    print(
        format_get_api_output(
            state_data=data,
            id=id,
            schema=resource_to_schema(resource),
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
    "--limit",
    default=DEFAULT_LIMIT,
    type=int,
    help=("Maximum number of entries to return. 100 by default."),
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
@PublicAPI(stability="alpha")
def ray_list(
    resource: str,
    format: str,
    filter: List[str],
    limit: int,
    detail: bool,
    timeout: float,
    address: str,
):
    """List all states of a given resource.

    Normally, summary APIs are recommended before listing all resources.

    The output schema is defined at :ref:`State API Schema section. <state-api-schema>`

    For example, the output schema of `ray list tasks` is
    :ref:`ray.experimental.state.common.TaskState <state-api-schema-task>`.

    Usage:

        List all actor information from the cluster.

        ```
        ray list actors
        ```

        List 50 actors from the cluster. The sorting order cannot be controlled.

        ```
        ray list actors --limit 50
        ```

        List 10 actors with state PENDING.

        ```
        ray list actors --limit 10 --filter "state=PENDING"
        ```

        List actors with yaml format.

        ```
        ray list actors --format yaml
        ```

        List actors with details. When --detail is specified, it might query
        more data sources to obtain data in details.

        ```
        ray list actors --detail
        ```

    The API queries one or more components from the cluster to obtain the data.
    The returned state snapshot could be stale, and it is not guaranteed to return
    the live data.

    The API can return partial or missing output upon the following scenarios.

    - When the API queries more than 1 component, if some of them fail,
      the API will return the partial result (with a suppressible warning).
    - When the API returns too many entries, the API
      will truncate the output. Currently, truncated data cannot be
      selected by users.

    Args:
        resource: The type of the resource to query.

    Raises:
        :ref:`RayStateApiException <state-api-exceptions>`
            if the CLI is failed to query the data.
    """
    # All resource names use '_' rather than '-'. But users options have '-'
    resource = StateResource(resource.replace("-", "_"))
    format = AvailableFormat(format)

    # Create the State API server and put it into context
    client = StateApiClient(address=address)

    filter = [_parse_filter(f) for f in filter]

    options = ListApiOptions(
        limit=limit,
        timeout=timeout,
        filters=filter,
        detail=detail,
    )

    # If errors occur, exceptions will be thrown. Empty data indicate successful query.
    try:
        data = client.list(
            resource,
            options=options,
            raise_on_missing_output=False,
            _explain=_should_explain(format),
        )
    except RayStateApiException as e:
        raise click.UsageError(str(e))

    # If --detail is given, the default formatting is yaml.
    if detail and format == AvailableFormat.DEFAULT:
        format = AvailableFormat.YAML

    # Print data to console.
    print(
        format_list_api_output(
            state_data=data,
            schema=resource_to_schema(resource),
            format=format,
        )
    )


@click.group("summary")
@click.pass_context
@PublicAPI(stability="alpha")
def summary_state_cli_group(ctx):
    """Return the summarized information of a given resource."""
    pass


@summary_state_cli_group.command(name="tasks")
@timeout_option
@address_option
@click.pass_context
@PublicAPI(stability="alpha")
def task_summary(ctx, timeout: float, address: str):
    """Summarize the task state of the cluster.

    By default, the output contains the information grouped by
    task function names.

    The output schema is
    :ref:`ray.experimental.state.common.TaskSummaries <state-api-schema-task-summary>`.

    Raises:
        :ref:`RayStateApiException <state-api-exceptions>`
            if the CLI is failed to query the data.
    """
    print(
        format_summary_output(
            summarize_tasks(
                address=address,
                timeout=timeout,
                raise_on_missing_output=False,
                _explain=True,
            ),
            resource=StateResource.TASKS,
        )
    )


@summary_state_cli_group.command(name="actors")
@timeout_option
@address_option
@click.pass_context
@PublicAPI(stability="alpha")
def actor_summary(ctx, timeout: float, address: str):
    """Summarize the actor state of the cluster.

    By default, the output contains the information grouped by
    actor class names.

    The output schema is
    :ref:`ray.experimental.state.common.ActorSummaries
    <state-api-schema-actor-summary>`.

    Raises:
        :ref:`RayStateApiException <state-api-exceptions>`
            if the CLI is failed to query the data.
    """
    print(
        format_summary_output(
            summarize_actors(
                address=address,
                timeout=timeout,
                raise_on_missing_output=False,
                _explain=True,
            ),
            resource=StateResource.ACTORS,
        )
    )


@summary_state_cli_group.command(name="objects")
@timeout_option
@address_option
@click.pass_context
@PublicAPI(stability="alpha")
def object_summary(ctx, timeout: float, address: str):
    """Summarize the object state of the cluster.

    The API is recommended when debugging memory leaks.
    See :ref:`Debugging with Ray Memory <debug-with-ray-memory>` for more details.
    (Note that this command is almost equivalent to `ray memory`, but it returns
    easier-to-understand output).

    By default, the output contains the information grouped by
    object callsite. Note that the callsite is not collected and
    all data will be aggregated as "disable" callsite if the env var
    `RAY_record_ref_creation_sites` is not configured. To enable the
    callsite collection, set the following environment variable when
    starting Ray.

    Example:

        ```
        RAY_record_ref_creation_sites=1 ray start --head
        ```

        ```
        RAY_record_ref_creation_sites=1 ray_script.py
        ```

    The output schema is
    :ref:`ray.experimental.state.common.ObjectSummaries
    <state-api-schema-object-summary>`.

    Raises:
        :ref:`RayStateApiException <state-api-exceptions>`
            if the CLI is failed to query the data.
    """
    print(
        format_object_summary_output(
            summarize_objects(
                address=address,
                timeout=timeout,
                raise_on_missing_output=False,
                _explain=True,
            ),
        )
    )

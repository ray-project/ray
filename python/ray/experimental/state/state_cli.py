import json
import logging
from datetime import datetime
from enum import Enum, unique
from typing import Dict, List, Optional, Tuple

import click
import yaml

import ray._private.services as services
from ray._private.profiling import get_perfetto_output
from ray._private.thirdparty.tabulate.tabulate import tabulate
from ray.experimental.state.api import (
    StateApiClient,
    get_log,
    list_logs,
    summarize_actors,
    summarize_objects,
    summarize_tasks,
)
from ray.experimental.state.common import (
    DEFAULT_LIMIT,
    DEFAULT_LOG_LIMIT,
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
    PERFETTO = "perfetto"


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
    elif format == AvailableFormat.PERFETTO:
        return get_perfetto_output(state_data)
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

    # Perfetto format requires detailed output.
    if format == AvailableFormat.PERFETTO:
        options.detail = True
        options.limit = 10000

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


log_follow_option = click.option(
    "--follow",
    "-f",
    required=False,
    type=bool,
    is_flag=True,
    help="Streams the log file as it is updated instead of just tailing.",
)

log_tail_option = click.option(
    "--tail",
    required=False,
    type=int,
    default=DEFAULT_LOG_LIMIT,
    help="Number of lines to tail from log. -1 indicates fetching the whole file.",
)

log_interval_option = click.option(
    "--interval",
    required=False,
    type=float,
    default=None,
    help="The interval in secs to print new logs when `--follow` is specified.",
    hidden=True,
)

log_timeout_option = click.option(
    "--timeout",
    default=DEFAULT_RPC_TIMEOUT,
    help=(
        "Timeout in seconds for the API requests. "
        f"Default is {DEFAULT_RPC_TIMEOUT}. If --follow is specified, "
        "this option will be ignored."
    ),
)

log_node_ip_option = click.option(
    "-ip",
    "--node-ip",
    required=False,
    type=str,
    default=None,
    help="Filters the logs by this ip address",
)

log_node_id_option = click.option(
    "--node-id",
    "-id",
    required=False,
    type=str,
    default=None,
    help="Filters the logs by this NodeID",
)

log_suffix_option = click.option(
    "--suffix",
    required=False,
    default="out",
    type=click.Choice(["out", "err"], case_sensitive=False),
    help=(
        "The suffix of the log file that denotes the log type, where out refers "
        "to logs from stdout, and err for logs from stderr "
    ),
)


def _get_head_node_ip(address: Optional[str] = None):
    """Get the head node ip from the ray address if possible

    Args:
        address: ray cluster address, e.g. "auto", "localhost:6379"

    Raises:
        click.UsageError if node ip could not be resolved
    """
    try:
        address = services.canonicalize_bootstrap_address_or_die(address)
        return address.split(":")[0]
    except (ConnectionError, ValueError) as e:
        # Hide all the stack trace
        raise click.UsageError(str(e))


def _print_log(
    address: Optional[str] = None,
    node_id: Optional[str] = None,
    node_ip: Optional[str] = None,
    filename: Optional[str] = None,
    actor_id: Optional[str] = None,
    pid: Optional[int] = None,
    follow: bool = False,
    tail: int = DEFAULT_LOG_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    interval: Optional[float] = None,
    suffix: Optional[str] = None,
):
    """Wrapper around `get_log()` that prints the preamble and the log lines"""
    if tail > 0:
        print(
            f"--- Log has been truncated to last {tail} lines."
            " Use `--tail` flag to toggle. Set to -1 for getting the entire file. ---\n"
        )

    if node_id is None and node_ip is None:
        # Auto detect node ip from the ray address when address neither is given
        node_ip = _get_head_node_ip(address)

    for chunk in get_log(
        address=address,
        node_id=node_id,
        node_ip=node_ip,
        filename=filename,
        actor_id=actor_id,
        tail=tail,
        pid=pid,
        follow=follow,
        _interval=interval,
        timeout=timeout,
        suffix=suffix,
    ):
        print(chunk, end="", flush=True)


LOG_CLI_HELP_MSG = """
Get logs based on filename (cluster) or resource identifiers (actor)

Example:

    Get all the log files available on a node (ray address could be
    obtained from `ray start --head` or `ray.init()`).

    ```
    ray logs cluster
    ```

    [ray logs cluster] Print the last 500 lines of raylet.out on a head node.

    ```
    ray logs cluster raylet.out --tail 500
    ```

    Or simply, using `ray logs` as an alias for `ray logs cluster`:

    ```
    ray logs raylet.out --tail 500
    ```

    Print the last 500 lines of raylet.out on a worker node id A.

    ```
    ray logs raylet.out --tail 500 —-node-id A
    ```

    [ray logs actor] Follow the log file with an actor id ABC.

    ```
    ray logs actor --id ABC --follow
    ```
"""


class LogCommandGroup(click.Group):
    def resolve_command(self, ctx, args):
        """Try resolve the command line args assuming users omitted the subcommand.

        This overrides the default `resolve_command` for the parent class.
        This will allow command alias of `ray <glob>` to `ray cluster <glob>`.
        """
        ctx.resilient_parsing = True
        res = super().resolve_command(ctx, args)
        cmd_name, cmd, parsed_args = res
        if cmd is None:
            # It could have been `ray logs ...`, forward to `ray logs cluster ...`
            return super().resolve_command(ctx, ["cluster"] + args)
        return cmd_name, cmd, parsed_args


logs_state_cli_group = LogCommandGroup(help=LOG_CLI_HELP_MSG)


@logs_state_cli_group.command(name="cluster")
@click.argument(
    "glob_filter",
    required=False,
    default="*",
)
@address_option
@log_node_id_option
@log_node_ip_option
@log_follow_option
@log_tail_option
@log_interval_option
@log_timeout_option
@click.pass_context
@PublicAPI(stability="alpha")
def log_cluster(
    ctx,
    glob_filter: str,
    address: Optional[str],
    node_id: Optional[str],
    node_ip: Optional[str],
    follow: bool,
    tail: int,
    interval: float,
    timeout: int,
):
    """Get/List logs that matches the GLOB_FILTER in the cluster.
    By default, it prints a list of log files that match the filter.
    By default, it prints the head node logs.
    If there's only 1 match, it will print the log file.

    Example:

        Print the last 500 lines of raylet.out on a head node.

        ```
        ray logs [cluster] raylet.out --tail 500
        ```

        Print the last 500 lines of raylet.out on a worker node id A.

        ```
        ray logs [cluster] raylet.out --tail 500 —-node-id A
        ```

        Download the gcs_server.txt file to the local machine.

        ```
        ray logs [cluster] gcs_server.out --tail -1 > gcs_server.txt
        ```

        Follow the log files from the last 100 lines.

        ```
        ray logs [cluster] raylet.out --tail 100 -f
        ```

    Raises:
        :ref:`RayStateApiException <state-api-exceptions>` if the CLI
            is failed to query the data.
    """

    if node_id is None and node_ip is None:
        node_ip = _get_head_node_ip(address)

    logs = list_logs(
        address=address,
        node_id=node_id,
        node_ip=node_ip,
        glob_filter=glob_filter,
        timeout=timeout,
    )

    log_files_found = []
    for _, log_files in logs.items():
        for log_file in log_files:
            log_files_found.append(log_file)

    if len(log_files_found) != 1:
        # Print the list of log files found if no unique log found
        if node_id:
            print(f"Node ID: {node_id}")
        elif node_ip:
            print(f"Node IP: {node_ip}")
        print(output_with_format(logs, schema=None, format=AvailableFormat.YAML))
        return

    # If there's only 1 file, that means there's a unique match.
    filename = log_files_found[0]

    _print_log(
        address=address,
        node_id=node_id,
        node_ip=node_ip,
        filename=filename,
        tail=tail,
        follow=follow,
        interval=interval,
        timeout=timeout,
    )


@logs_state_cli_group.command(name="actor")
@click.option(
    "--id",
    "-a",
    required=False,
    type=str,
    default=None,
    help="Retrieves the logs corresponding to this ActorID.",
)
@click.option(
    "--pid",
    "-pid",
    required=False,
    type=str,
    default=None,
    help="Retrieves the logs from the actor with this pid.",
)
@address_option
@log_node_id_option
@log_node_ip_option
@log_follow_option
@log_tail_option
@log_interval_option
@log_timeout_option
@log_suffix_option
@click.pass_context
@PublicAPI(stability="alpha")
def log_actor(
    ctx,
    id: Optional[str],
    pid: Optional[str],
    address: Optional[str],
    node_id: Optional[str],
    node_ip: Optional[str],
    follow: bool,
    tail: int,
    interval: float,
    timeout: int,
    suffix: str,
):
    """Get/List logs associated with an actor.

    Example:

        Follow the log file with an actor id ABC.

        ```
        ray logs actor --id ABC --follow
        ```

        Get the actor log from pid 123, ip ABC.
        Note that this goes well with the driver log of Ray which prints
        (ip=ABC, pid=123, class_name) logs.

        ```
        ray logs actor --pid=123  —ip=ABC
        ```

        Get the actor err log file.

        ```
        ray logs actor --id ABC --suffix err
        ```

    Raises:
        :ref:`RayStateApiException <state-api-exceptions>`
            if the CLI is failed to query the data.
        MissingParameter if inputs are missing.
    """

    if pid is None and id is None:
        raise click.MissingParameter(
            message="At least one of `--pid` and `--id` has to be set",
            param_type="option",
        )

    _print_log(
        address=address,
        node_id=node_id,
        node_ip=node_ip,
        pid=pid,
        actor_id=id,
        tail=tail,
        follow=follow,
        interval=interval,
        timeout=timeout,
        suffix=suffix,
    )


@logs_state_cli_group.command(name="worker")
@click.option(
    "--pid",
    "-pid",
    # The only identifier supported for now, TODO(rickyx): add worker id support
    required=True,
    type=str,
    help="Retrieves the logs from the worker with this pid.",
)
@address_option
@log_node_id_option
@log_node_ip_option
@log_follow_option
@log_tail_option
@log_interval_option
@log_timeout_option
@log_suffix_option
@click.pass_context
@PublicAPI(stability="alpha")
def log_worker(
    ctx,
    pid: Optional[str],
    address: Optional[str],
    node_id: Optional[str],
    node_ip: Optional[str],
    follow: bool,
    tail: int,
    interval: float,
    timeout: int,
    suffix: str,
):
    """Get/List logs associated with a worker process.

    Example:

        Follow the log file from a worker process with pid=ABC.

        ```
        ray logs worker --pid ABC --follow
        ```

        Get the stderr logs from a worker process.

        ```
        ray logs worker --pid ABC --suffix err
        ```

    Raises:
        :ref:`RayStateApiException <state-api-exceptions>`
            if the CLI is failed to query the data.
        MissingParameter if inputs are missing.
    """

    _print_log(
        address=address,
        node_id=node_id,
        node_ip=node_ip,
        pid=pid,
        tail=tail,
        follow=follow,
        interval=interval,
        timeout=timeout,
        suffix=suffix,
    )

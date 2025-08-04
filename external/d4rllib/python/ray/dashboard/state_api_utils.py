import dataclasses
from dataclasses import asdict, fields
from typing import Awaitable, Callable, List, Tuple

import aiohttp.web

from ray.dashboard.optional_utils import rest_response
from ray.dashboard.utils import HTTPStatusCode
from ray.util.state.common import (
    DEFAULT_LIMIT,
    DEFAULT_RPC_TIMEOUT,
    RAY_MAX_LIMIT_FROM_API_SERVER,
    ListApiOptions,
    ListApiResponse,
    PredicateType,
    StateSchema,
    SummaryApiOptions,
    SummaryApiResponse,
    SupportedFilterType,
    filter_fields,
)
from ray.util.state.exception import DataSourceUnavailable
from ray.util.state.util import convert_string_to_type


def do_reply(success: bool, error_message: str, result: ListApiResponse, **kwargs):
    return rest_response(
        status_code=HTTPStatusCode.OK if success else HTTPStatusCode.INTERNAL_ERROR,
        message=error_message,
        result=result,
        convert_google_style=False,
        **kwargs,
    )


async def handle_list_api(
    list_api_fn: Callable[[ListApiOptions], Awaitable[ListApiResponse]],
    req: aiohttp.web.Request,
):
    try:
        result = await list_api_fn(option=options_from_req(req))
        return do_reply(
            success=True,
            error_message="",
            result=asdict(result),
        )
    except DataSourceUnavailable as e:
        return do_reply(success=False, error_message=str(e), result=None)


def _get_filters_from_req(
    req: aiohttp.web.Request,
) -> List[Tuple[str, PredicateType, SupportedFilterType]]:
    filter_keys = req.query.getall("filter_keys", [])
    filter_predicates = req.query.getall("filter_predicates", [])
    filter_values = req.query.getall("filter_values", [])
    assert len(filter_keys) == len(filter_values)
    filters = []
    for key, predicate, val in zip(filter_keys, filter_predicates, filter_values):
        filters.append((key, predicate, val))
    return filters


def options_from_req(req: aiohttp.web.Request) -> ListApiOptions:
    """Obtain `ListApiOptions` from the aiohttp request."""
    limit = int(
        req.query.get("limit") if req.query.get("limit") is not None else DEFAULT_LIMIT
    )

    if limit > RAY_MAX_LIMIT_FROM_API_SERVER:
        raise ValueError(
            f"Given limit {limit} exceeds the supported "
            f"limit {RAY_MAX_LIMIT_FROM_API_SERVER}. Use a lower limit."
        )

    timeout = int(req.query.get("timeout", 30))
    filters = _get_filters_from_req(req)
    detail = convert_string_to_type(req.query.get("detail", False), bool)
    exclude_driver = convert_string_to_type(req.query.get("exclude_driver", True), bool)

    return ListApiOptions(
        limit=limit,
        timeout=timeout,
        filters=filters,
        detail=detail,
        exclude_driver=exclude_driver,
    )


def summary_options_from_req(req: aiohttp.web.Request) -> SummaryApiOptions:
    timeout = int(req.query.get("timeout", DEFAULT_RPC_TIMEOUT))
    filters = _get_filters_from_req(req)
    summary_by = req.query.get("summary_by", None)
    return SummaryApiOptions(timeout=timeout, filters=filters, summary_by=summary_by)


async def handle_summary_api(
    summary_fn: Callable[[SummaryApiOptions], SummaryApiResponse],
    req: aiohttp.web.Request,
):
    result = await summary_fn(option=summary_options_from_req(req))
    return do_reply(
        success=True,
        error_message="",
        result=asdict(result),
    )


def convert_filters_type(
    filter: List[Tuple[str, PredicateType, SupportedFilterType]],
    schema: StateSchema,
) -> List[Tuple[str, PredicateType, SupportedFilterType]]:
    """Convert the given filter's type to SupportedFilterType.

    This method is necessary because click can only accept a single type
    for its tuple (which is string in this case).

    Args:
        filter: A list of filter which is a tuple of (key, val).
        schema: The state schema. It is used to infer the type of the column for filter.

    Returns:
        A new list of filters with correct types that match the schema.
    """
    new_filter = []
    if dataclasses.is_dataclass(schema):
        schema = {field.name: field.type for field in fields(schema)}
    else:
        schema = schema.schema_dict()

    for col, predicate, val in filter:
        if col in schema:
            column_type = schema[col]
            try:
                isinstance(val, column_type)
            except TypeError:
                # Calling `isinstance` to the Literal type raises a TypeError.
                # Ignore this case.
                pass
            else:
                if isinstance(val, column_type):
                    # Do nothing.
                    pass
                elif column_type is int or column_type == "integer":
                    try:
                        val = convert_string_to_type(val, int)
                    except ValueError:
                        raise ValueError(
                            f"Invalid filter `--filter {col} {val}` for a int type "
                            "column. Please provide an integer filter "
                            f"`--filter {col} [int]`"
                        )
                elif column_type is float or column_type == "number":
                    try:
                        val = convert_string_to_type(
                            val,
                            float,
                        )
                    except ValueError:
                        raise ValueError(
                            f"Invalid filter `--filter {col} {val}` for a float "
                            "type column. Please provide an integer filter "
                            f"`--filter {col} [float]`"
                        )
                elif column_type is bool or column_type == "boolean":
                    try:
                        val = convert_string_to_type(val, bool)
                    except ValueError:
                        raise ValueError(
                            f"Invalid filter `--filter {col} {val}` for a boolean "
                            "type column. Please provide "
                            f"`--filter {col} [True|true|1]` for True or "
                            f"`--filter {col} [False|false|0]` for False."
                        )
        new_filter.append((col, predicate, val))
    return new_filter


def do_filter(
    data: List[dict],
    filters: List[Tuple[str, PredicateType, SupportedFilterType]],
    state_dataclass: StateSchema,
    detail: bool,
) -> List[dict]:
    """Return the filtered data given filters.

    Args:
        data: A list of state data.
        filters: A list of KV tuple to filter data (key, val). The data is filtered
            if data[key] != val.
        state_dataclass: The state schema.

    Returns:
        A list of filtered state data in dictionary. Each state data's
        unnecessary columns are filtered by the given state_dataclass schema.
    """
    filters = convert_filters_type(filters, state_dataclass)
    result = []
    for datum in data:
        match = True
        for filter_column, filter_predicate, filter_value in filters:
            filterable_columns = state_dataclass.filterable_columns()
            filter_column = filter_column.lower()
            if filter_column not in filterable_columns:
                raise ValueError(
                    f"The given filter column {filter_column} is not supported. "
                    "Enter filters with –-filter key=value "
                    "or –-filter key!=value "
                    f"Supported filter columns: {filterable_columns}"
                )

            if filter_column not in datum:
                match = False
            elif filter_predicate == "=":
                if isinstance(filter_value, str) and isinstance(
                    datum[filter_column], str
                ):
                    # Case insensitive match for string filter values.
                    match = datum[filter_column].lower() == filter_value.lower()
                elif isinstance(filter_value, str) and isinstance(
                    datum[filter_column], bool
                ):
                    match = datum[filter_column] == convert_string_to_type(
                        filter_value, bool
                    )
                elif isinstance(filter_value, str) and isinstance(
                    datum[filter_column], int
                ):
                    match = datum[filter_column] == convert_string_to_type(
                        filter_value, int
                    )
                else:
                    match = datum[filter_column] == filter_value
            elif filter_predicate == "!=":
                if isinstance(filter_value, str) and isinstance(
                    datum[filter_column], str
                ):
                    match = datum[filter_column].lower() != filter_value.lower()
                else:
                    match = datum[filter_column] != filter_value
            else:
                raise ValueError(
                    f"Unsupported filter predicate {filter_predicate} is given. "
                    "Available predicates: =, !=."
                )

            if not match:
                break

        if match:
            result.append(filter_fields(datum, state_dataclass, detail))
    return result

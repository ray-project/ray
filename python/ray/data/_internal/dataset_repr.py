import itertools
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import numpy as np

import ray
from ray.data._internal.logical.interfaces import SourceOperator
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import (
    DEFAULT_DATASET_REPR_HEAD_COLUMNS,
    DEFAULT_DATASET_REPR_HEAD_ROWS,
    DEFAULT_DATASET_REPR_MAX_BYTES_LENGTH,
    DEFAULT_DATASET_REPR_MAX_COLLECTION_ITEMS,
    DEFAULT_DATASET_REPR_MAX_COLUMN_WIDTH,
    DEFAULT_DATASET_REPR_MAX_COLUMNS,
    DEFAULT_DATASET_REPR_MAX_ROWS,
    DEFAULT_DATASET_REPR_MAX_STRING_LENGTH,
    DEFAULT_DATASET_REPR_MAX_TENSOR_ELEMENTS,
    DataContext,
)
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data.dataset import Dataset, Schema

_DATASET_REPR_ELLIPSIS = "…"
_DATASET_REPR_PREVIEW_MAX_DEPTH = 2

__all__ = [
    "_build_dataset_ascii_repr",
    "_dataset_repr_config_from_context",
]


@dataclass
class _DatasetReprConfig:
    max_rows: int
    head_rows: int
    max_columns: int
    head_columns: int
    max_column_width: int
    max_string_length: int
    max_bytes_length: int
    max_collection_items: int
    max_tensor_elements: int


def _dataset_repr_config_from_context(
    context: Optional[DataContext],
) -> _DatasetReprConfig:
    def _get_value(name: str, default: int) -> int:
        if context is None:
            return default
        value = getattr(context, name, default)
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    return _DatasetReprConfig(
        max_rows=_get_value("dataset_repr_max_rows", DEFAULT_DATASET_REPR_MAX_ROWS),
        head_rows=_get_value("dataset_repr_head_rows", DEFAULT_DATASET_REPR_HEAD_ROWS),
        max_columns=_get_value(
            "dataset_repr_max_columns", DEFAULT_DATASET_REPR_MAX_COLUMNS
        ),
        head_columns=_get_value(
            "dataset_repr_head_columns", DEFAULT_DATASET_REPR_HEAD_COLUMNS
        ),
        max_column_width=_get_value(
            "dataset_repr_max_column_width", DEFAULT_DATASET_REPR_MAX_COLUMN_WIDTH
        ),
        max_string_length=_get_value(
            "dataset_repr_max_string_length", DEFAULT_DATASET_REPR_MAX_STRING_LENGTH
        ),
        max_bytes_length=_get_value(
            "dataset_repr_max_bytes_length", DEFAULT_DATASET_REPR_MAX_BYTES_LENGTH
        ),
        max_collection_items=_get_value(
            "dataset_repr_max_collection_items",
            DEFAULT_DATASET_REPR_MAX_COLLECTION_ITEMS,
        ),
        max_tensor_elements=_get_value(
            "dataset_repr_max_tensor_elements",
            DEFAULT_DATASET_REPR_MAX_TENSOR_ELEMENTS,
        ),
    )


@dataclass
class _ColumnDisplaySpec:
    name: Optional[str]
    dtype: Optional[str]
    is_gap: bool = False


def _prepare_columns_for_repr(
    columns: List[str],
    dtype_strings: List[str],
    config: _DatasetReprConfig,
) -> Tuple[List[_ColumnDisplaySpec], bool, int]:
    if not columns:
        return [], False, 0

    max_columns = config.max_columns
    if max_columns is None or max_columns <= 0:
        max_columns = len(columns)
    max_columns = max(1, max_columns)

    num_columns = len(columns)
    display: List[_ColumnDisplaySpec] = []

    if num_columns <= max_columns:
        for name, dtype in zip(columns, dtype_strings):
            display.append(_ColumnDisplaySpec(name=name, dtype=dtype))
        return display, False, len(display)

    if max_columns == 1:
        display.append(_ColumnDisplaySpec(name=columns[0], dtype=dtype_strings[0]))
        return display, True, 1

    head_target = min(config.head_columns, max_columns - 1)
    if head_target < 0:
        head_target = 0
    gap_needed = head_target > 0
    tail_slots = max_columns - head_target - (1 if gap_needed else 0)
    if tail_slots < 0:
        tail_slots = 0

    for idx in range(head_target):
        display.append(_ColumnDisplaySpec(name=columns[idx], dtype=dtype_strings[idx]))

    if gap_needed:
        display.append(_ColumnDisplaySpec(name=None, dtype=None, is_gap=True))

    tail_start = max(num_columns - tail_slots, head_target)
    for idx in range(tail_start, num_columns):
        if len(display) >= max_columns:
            break
        display.append(_ColumnDisplaySpec(name=columns[idx], dtype=dtype_strings[idx]))

    shown_columns = sum(1 for spec in display if not spec.is_gap)
    return display, True, shown_columns


def _build_dataset_ascii_repr(
    dataset: "Dataset",
    schema: "Schema",
    is_materialized: bool,
    config: _DatasetReprConfig,
) -> str:
    columns = list(schema.names)
    if not columns:
        return dataset._plan.get_plan_as_string(dataset.__class__)

    dtype_strings = [_repr_format_dtype(t) for t in schema.types]
    (
        display_specs,
        columns_truncated,
        num_columns_shown,
    ) = _prepare_columns_for_repr(columns, dtype_strings, config)

    column_headers: List[str] = []
    dtype_headers: List[str] = []
    preview_column_names: List[Optional[str]] = []
    for spec in display_specs:
        header_value = spec.name if spec.name is not None else _DATASET_REPR_ELLIPSIS
        dtype_value = spec.dtype if spec.dtype is not None else _DATASET_REPR_ELLIPSIS
        column_headers.append(
            _truncate_to_cell_width(str(header_value), config.max_column_width)
        )
        dtype_headers.append(
            _truncate_to_cell_width(str(dtype_value), config.max_column_width)
        )
        preview_column_names.append(spec.name)

    separator_row = ["---"] * len(display_specs)

    num_rows = dataset._meta_count()
    num_cols = len(columns)
    shape_line = f"shape: ({num_rows if num_rows is not None else '?'}, {num_cols})"

    head_rows: List[List[str]] = []
    tail_rows: List[List[str]] = []
    rows_shown = 0
    show_gap = False
    if is_materialized:
        (
            head_data,
            tail_data,
            show_gap,
        ) = _collect_materialized_rows_for_repr(dataset, num_rows, config)
        head_rows = _format_rows_for_repr(head_data, preview_column_names, config)
        tail_rows = _format_rows_for_repr(tail_data, preview_column_names, config)
        rows_shown = len(head_rows) + len(tail_rows)

    display_rows: List[List[str]] = []
    display_rows.extend(head_rows)
    if show_gap:
        display_rows.append([_DATASET_REPR_ELLIPSIS] * len(display_specs))
    display_rows.extend(tail_rows)

    column_widths = _compute_column_widths(
        column_headers, dtype_headers, separator_row, display_rows
    )

    table_lines = _render_table_lines(
        column_headers,
        dtype_headers,
        separator_row,
        display_rows,
        column_widths,
    )

    summary_line = (
        f"(Showing {rows_shown} of {num_rows} rows)"
        if is_materialized
        else "(Dataset isn't materialized)"
    )
    if is_materialized and num_rows is None:
        summary_line = f"(Showing {rows_shown} of ? rows)"

    summary_lines = [summary_line]
    if columns_truncated:
        summary_lines.append(f"(Showing {num_columns_shown} of {num_cols} columns)")

    components = [shape_line, "\n".join(table_lines), "\n".join(summary_lines)]
    return "\n".join(components)


def _repr_format_dtype(dtype: Any) -> str:
    if hasattr(dtype, "__name__"):
        return dtype.__name__
    return str(dtype)


def _collect_materialized_rows_for_repr(
    dataset: "Dataset",
    num_rows: Optional[int],
    config: _DatasetReprConfig,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool]:
    block_entries: List[Tuple[ObjectRef, BlockMetadata]] = []
    snapshot = dataset._plan._snapshot_bundle
    if snapshot is not None:
        block_entries.extend(snapshot.blocks)
    else:
        dag = dataset._logical_plan.dag
        if isinstance(dag, SourceOperator):
            input_data = dag.output_data() or []
            for bundle in input_data:
                block_entries.extend(bundle.blocks)

    if not block_entries:
        return [], [], False

    head_target, tail_target = _determine_preview_row_targets(num_rows, config)
    block_cache: Dict[ObjectRef, Block] = {}

    def _resolve_block(block_ref: ObjectRef) -> Block:
        if block_ref not in block_cache:
            block_cache[block_ref] = ray.get(block_ref)
        return block_cache[block_ref]

    head_rows: List[Dict[str, Any]] = []
    head_remaining = head_target
    for block_ref, _ in block_entries:
        if head_remaining <= 0:
            break
        block = _resolve_block(block_ref)
        accessor = BlockAccessor.for_block(block)
        for row in accessor.iter_rows(public_row_format=True):
            head_rows.append(row)
            head_remaining -= 1
            if head_remaining <= 0:
                break

    tail_rows: List[Dict[str, Any]] = []
    tail_remaining = tail_target
    tail_parts: List[List[Dict[str, Any]]] = []
    if tail_remaining > 0:
        for block_ref, metadata in reversed(block_entries):
            if tail_remaining <= 0:
                break
            block = _resolve_block(block_ref)
            accessor = BlockAccessor.for_block(block)
            total_rows = metadata.num_rows
            if total_rows is None:
                total_rows = accessor.num_rows()
            if total_rows == 0:
                continue
            start = max(0, total_rows - tail_remaining)
            sliced_block = accessor.slice(start, total_rows, copy=False)
            slice_accessor = BlockAccessor.for_block(sliced_block)
            block_rows = list(slice_accessor.iter_rows(public_row_format=True))
            tail_parts.append(block_rows)
            tail_remaining -= len(block_rows)
            if tail_remaining <= 0:
                break

    for part in reversed(tail_parts):
        tail_rows.extend(part)

    show_gap = bool(head_rows) and bool(tail_rows)
    return head_rows, tail_rows, show_gap


def _determine_preview_row_targets(
    num_rows: Optional[int],
    config: _DatasetReprConfig,
) -> Tuple[int, int]:
    max_rows = config.max_rows
    # Respect the overall ``max_rows`` budget first; if ``num_rows`` is known and
    # smaller than the budget we simply show everything. Otherwise we split the
    # budget into a head (first N rows) and tail (last M rows) preview, so users
    # get a glimpse of both ends while keeping the number of fetched rows bounded.
    if max_rows is None or max_rows <= 0:
        return 0, 0

    if num_rows is None or num_rows <= max_rows:
        head = num_rows if num_rows is not None else max_rows
        return head, 0

    head = min(config.head_rows, max_rows)
    if head < 0:
        head = 0
    tail = max_rows - head
    if tail < 0:
        tail = 0
    return head, tail


def _format_rows_for_repr(
    rows: List[Dict[str, Any]],
    column_names: List[Optional[str]],
    config: _DatasetReprConfig,
) -> List[List[str]]:
    formatted_rows: List[List[str]] = []
    for row in rows:
        formatted_row = []
        for column in column_names:
            if column is None:
                formatted_row.append(_DATASET_REPR_ELLIPSIS)
                continue
            value = row.get(column)
            formatted_value = _repr_format_value(value, config)
            formatted_row.append(
                _truncate_to_cell_width(formatted_value, config.max_column_width)
            )
        formatted_rows.append(formatted_row)
    return formatted_rows


def _truncate_to_cell_width(value: str, max_width: int) -> str:
    if max_width is None:
        return value
    if max_width <= 0:
        return _DATASET_REPR_ELLIPSIS if value else ""
    if len(value) <= max_width:
        return value
    if max_width == 1:
        return _DATASET_REPR_ELLIPSIS
    return value[: max_width - 1] + _DATASET_REPR_ELLIPSIS


def _repr_format_value(
    value: Any,
    config: _DatasetReprConfig,
    depth: int = 0,
) -> str:
    if depth >= _DATASET_REPR_PREVIEW_MAX_DEPTH:
        return _DATASET_REPR_ELLIPSIS

    if isinstance(value, np.generic):
        value = value.item()
    if value is None:
        return "None"
    if isinstance(value, str):
        return _truncate_text_value(value, config.max_string_length)
    if isinstance(value, (bytes, bytearray, memoryview)):
        if isinstance(value, memoryview):
            value = value.tobytes()
        elif isinstance(value, bytearray):
            value = bytes(value)
        return _format_binary_value(value, config)
    if isinstance(value, np.ndarray):
        return _format_ndarray_value(value, config, depth)
    if isinstance(value, dict):
        return _format_mapping_value(value, config, depth)
    if isinstance(value, (list, tuple, set, frozenset)):
        return _format_sequence_value(value, config, depth)
    return str(value)


def _truncate_text_value(value: str, limit: int) -> str:
    if limit is None:
        return value
    if limit < 0:
        return value
    if limit == 0:
        return _DATASET_REPR_ELLIPSIS if value else ""
    if len(value) <= limit:
        return value
    return value[:limit] + _DATASET_REPR_ELLIPSIS


def _format_binary_value(value: bytes, config: _DatasetReprConfig) -> str:
    try:
        decoded = value.decode()
        return _truncate_text_value(decoded, config.max_bytes_length)
    except Exception:
        return _truncate_text_value(repr(value), config.max_bytes_length)


def _format_sequence_value(
    value: Union[List[Any], Tuple[Any, ...], set, frozenset],
    config: _DatasetReprConfig,
    depth: int,
) -> str:
    limit = config.max_collection_items
    preview, truncated = _preview_iterable(value, limit)
    rendered = [_repr_format_value(item, config, depth + 1) for item in preview]
    body = ", ".join(rendered)
    if truncated:
        body = f"{body}, {_DATASET_REPR_ELLIPSIS}" if body else _DATASET_REPR_ELLIPSIS

    if isinstance(value, tuple):
        if not truncated and len(preview) == 1:
            body = f"{body},"
        return f"({body})"

    if isinstance(value, list):
        return f"[{body}]"

    if isinstance(value, set):
        if not body and not truncated:
            return "set()"
        return f"{{{body}}}"

    if isinstance(value, frozenset):
        if not body and not truncated:
            return "frozenset()"
        return f"frozenset({{{body}}})"

    return f"[{body}]"


def _format_mapping_value(
    mapping: Mapping[Any, Any],
    config: _DatasetReprConfig,
    depth: int,
) -> str:
    limit = config.max_collection_items
    preview_items, truncated = _preview_iterable(mapping.items(), limit)
    rendered = []
    for key, val in preview_items:
        key_repr = _repr_format_value(key, config, depth + 1)
        val_repr = _repr_format_value(val, config, depth + 1)
        rendered.append(f"{key_repr}: {val_repr}")

    body = ", ".join(rendered)
    if truncated:
        body = f"{body}, {_DATASET_REPR_ELLIPSIS}" if body else _DATASET_REPR_ELLIPSIS
    return f"{{{body}}}"


def _format_ndarray_value(
    array: np.ndarray,
    config: _DatasetReprConfig,
    depth: int,
) -> str:
    max_elements = config.max_tensor_elements
    if max_elements is None or max_elements < 0:
        max_elements = array.size
    preview = list(itertools.islice(array.flat, max_elements))
    truncated = array.size > max_elements
    rendered = [_repr_format_value(val, config, depth + 1) for val in preview]
    body = ", ".join(rendered)
    if truncated:
        body = f"{body}, {_DATASET_REPR_ELLIPSIS}" if body else _DATASET_REPR_ELLIPSIS
    return f"array(shape={array.shape}, dtype={array.dtype}, data=[{body}])"


def _preview_iterable(values: Iterable[Any], limit: int) -> Tuple[List[Any], bool]:
    iterator = iter(values)
    if limit is None or limit < 0:
        return list(iterator), False
    if limit == 0:
        try:
            next(iterator)
            return [], True
        except StopIteration:
            return [], False

    preview = list(itertools.islice(iterator, limit))
    try:
        next(iterator)
        truncated = True
    except StopIteration:
        truncated = False
    return preview, truncated


def _compute_column_widths(
    headers: List[str],
    dtype_headers: List[str],
    separator_row: List[str],
    data_rows: List[List[str]],
) -> List[int]:
    column_widths: List[int] = []
    for idx in range(len(headers)):
        widths = [
            len(headers[idx]),
            len(dtype_headers[idx]),
            len(separator_row[idx]),
        ]
        for row in data_rows:
            widths.append(len(row[idx]))
        column_widths.append(max(widths))
    return column_widths


def _render_table_lines(
    headers: List[str],
    dtype_headers: List[str],
    separator_row: List[str],
    data_rows: List[List[str]],
    column_widths: List[int],
) -> List[str]:
    lines: List[str] = []
    top = _render_border("╭", "┬", "╮", "─", column_widths)
    header_row = _render_row(headers, column_widths)
    separator_line = _render_row(separator_row, column_widths)
    dtype_row = _render_row(dtype_headers, column_widths)
    lines.extend([top, header_row, separator_line, dtype_row])

    if data_rows:
        middle = _render_border("╞", "╪", "╡", "═", column_widths)
        lines.append(middle)
        for row in data_rows:
            lines.append(_render_row(row, column_widths))

    bottom = _render_border("╰", "┴", "╯", "─", column_widths)
    lines.append(bottom)
    return lines


def _render_border(
    left: str, middle: str, right: str, fill: str, column_widths: List[int]
) -> str:
    segments = [fill * (width + 2) for width in column_widths]
    return f"{left}{middle.join(segments)}{right}"


def _render_row(values: List[str], column_widths: List[int]) -> str:
    cells = []
    for idx, value in enumerate(values):
        padded = value.ljust(column_widths[idx])
        cells.append(f" {padded} ")
    return f"│{'┆'.join(cells)}│"

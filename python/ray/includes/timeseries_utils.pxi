# cython: profile=False
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False

# =============================================================================
# WARNING: This file is used EXCLUSIVELY by Ray Serve.
# =============================================================================
#
# These Cython-optimized timeseries utilities exist solely to support the
# Ray Serve controller's autoscaling metrics pipeline (specifically the
# per-deployment timeseries aggregation in the Serve replica scheduler).
#
# This code lives in `ray/includes/` and is compiled into `_raylet.so` because
# Rather than introducing a new top-level `.pyx` / `.so` for a single
# Serve-internal optimization, we include it here alongside the other
# `.pxi` helpers that ship in `_raylet`.
#
# If you are not working on Ray Serve autoscaling, you almost certainly do not
# need to modify this file.
# =============================================================================



# C library imports
from libc.stdlib cimport malloc, free
from libc.math cimport round as c_round, isnan, nan, isinf

# Heap node for k-way merge
cdef struct _TsHeapNode:
    double timestamp
    int series_idx
    double value
    int position_in_series  # Current position within the series


cdef inline void _ts_heap_sift_down(_TsHeapNode* heap, int size, int pos) noexcept nogil:
    """Sift down operation for min-heap (inline for performance)."""
    cdef int smallest, left, right
    cdef _TsHeapNode temp

    while True:
        smallest = pos
        left = 2 * pos + 1
        right = 2 * pos + 2

        if left < size and heap[left].timestamp < heap[smallest].timestamp:
            smallest = left

        if right < size and heap[right].timestamp < heap[smallest].timestamp:
            smallest = right

        if smallest == pos:
            break

        # Swap
        temp = heap[pos]
        heap[pos] = heap[smallest]
        heap[smallest] = temp
        pos = smallest


cdef inline void _ts_heap_sift_up(_TsHeapNode* heap, int pos) noexcept nogil:
    """Sift up operation for min-heap (inline for performance)."""
    cdef int parent
    cdef _TsHeapNode temp

    while pos > 0:
        parent = (pos - 1) // 2
        if heap[parent].timestamp <= heap[pos].timestamp:
            break

        # Swap
        temp = heap[pos]
        heap[pos] = heap[parent]
        heap[parent] = temp
        pos = parent


cdef inline void _ts_heap_pop(_TsHeapNode* heap, int* size) noexcept nogil:
    """Remove minimum element from heap."""
    if size[0] <= 0:
        return

    heap[0] = heap[size[0] - 1]
    size[0] -= 1
    if size[0] > 0:
        _ts_heap_sift_down(heap, size[0], 0)


cdef inline void _ts_heap_push(_TsHeapNode* heap, int* size, _TsHeapNode node) noexcept nogil:
    """Add element to heap."""
    heap[size[0]] = node
    _ts_heap_sift_up(heap, size[0])
    size[0] += 1


cdef int _kway_merge_timeseries_nogil(double** timestamps_arrays, double** values_arrays,
                              int* series_lengths, int num_series,
                              int result_capacity,
                              double** out_timestamps, double** out_values) noexcept nogil:
    """
    Fully nogil k-way merge operating on C arrays.

    Assumptions:
        - Each input series is sorted by timestamp in ascending order
        - Values represent instantaneous gauge measurements (non-negative)

    Args:
        timestamps_arrays: Array of pointers to timestamp arrays for each series
        values_arrays: Array of pointers to value arrays for each series
        series_lengths: Array of lengths for each series
        num_series: Number of series to merge
        result_capacity: Pre-allocated capacity (should be >= sum of all series lengths)
        out_timestamps: Output pointer for result timestamps
        out_values: Output pointer for result values

    Returns: Number of points in merged result, or -1 on error
    """
    cdef:
        int i, pos_in_series, series_idx
        int heap_size = 0
        double timestamp, value, old_value
        double running_total = 0.0
        double rounded_timestamp, last_rounded_timestamp = -1.0
        _TsHeapNode new_node
        int result_count = 0
        # C arrays for performance
        double* current_values = <double*>malloc(num_series * sizeof(double))
        int* series_positions = <int*>malloc(num_series * sizeof(int))
        _TsHeapNode* merge_heap = <_TsHeapNode*>malloc(num_series * sizeof(_TsHeapNode))
        double* result_timestamps = <double*>malloc(result_capacity * sizeof(double))
        double* result_values = <double*>malloc(result_capacity * sizeof(double))

    if not current_values or not series_positions or not merge_heap or not result_timestamps or not result_values:
        # Memory allocation failed
        if current_values:
            free(current_values)
        if series_positions:
            free(series_positions)
        if merge_heap:
            free(merge_heap)
        if result_timestamps:
            free(result_timestamps)
        if result_values:
            free(result_values)
        return -1

    # Initialize arrays
    for i in range(num_series):
        current_values[i] = 0.0
        series_positions[i] = 0

        # Push first element from each series to heap
        if series_lengths[i] > 0:
            merge_heap[heap_size].timestamp = timestamps_arrays[i][0]
            merge_heap[heap_size].series_idx = i
            merge_heap[heap_size].value = values_arrays[i][0]
            merge_heap[heap_size].position_in_series = 0
            heap_size += 1

    # Build initial heap
    for i in range(heap_size // 2 - 1, -1, -1):
        _ts_heap_sift_down(merge_heap, heap_size, i)

    # K-way merge
    while heap_size > 0:
        # Get minimum element
        timestamp = merge_heap[0].timestamp
        series_idx = merge_heap[0].series_idx
        value = merge_heap[0].value
        pos_in_series = merge_heap[0].position_in_series

        # Update running total
        old_value = current_values[series_idx]
        current_values[series_idx] = value
        running_total += value - old_value

        # Remove from heap
        _ts_heap_pop(merge_heap, &heap_size)

        # Push next element from same series if available
        series_positions[series_idx] = pos_in_series + 1
        if series_positions[series_idx] < series_lengths[series_idx]:
            new_node.timestamp = timestamps_arrays[series_idx][series_positions[series_idx]]
            new_node.series_idx = series_idx
            new_node.value = values_arrays[series_idx][series_positions[series_idx]]
            new_node.position_in_series = series_positions[series_idx]

            _ts_heap_push(merge_heap, &heap_size, new_node)

        # Only add point if value changed
        if value != old_value:
            # Round to 10ms precision
            rounded_timestamp = c_round(timestamp * 100.0) / 100.0

            # Check if we can merge with last point
            if result_count > 0 and last_rounded_timestamp == rounded_timestamp:
                # Update last point's value
                result_values[result_count - 1] = running_total
            else:
                # Add new point (capacity is pre-allocated to be large enough)
                result_timestamps[result_count] = rounded_timestamp
                result_values[result_count] = running_total
                result_count += 1
                last_rounded_timestamp = rounded_timestamp

    # Clean up
    free(current_values)
    free(series_positions)
    free(merge_heap)

    # Return results
    out_timestamps[0] = result_timestamps
    out_values[0] = result_values
    return result_count


def merge_instantaneous_total_cython(list replicas_timeseries):
    """
    Cython-optimized k-way merge for timeseries.

    This is a drop-in replacement for the Python version with 5-10x speedup.

    Assumptions:
        - Each input timeseries is sorted by timestamp in ascending order
        - Values represent instantaneous gauge measurements

    Args:
        replicas_timeseries: List of timeseries. Each timeseries is a list of
            objects with .timestamp and .value attributes.

    Returns:
        List of (timestamp, value) tuples representing the merged timeseries.
    """
    # Filter empty series
    cdef list active_series = [series for series in replicas_timeseries if series]

    if not active_series:
        return []

    if len(active_series) == 1:
        # Convert to tuples for consistent return type
        return [(point.timestamp, point.value) for point in active_series[0]]

    cdef:
        int num_series = len(active_series)
        int i, j
        int total_points = 0
        object point, series
        bint alloc_failed = False
        # C arrays for all timestamps and values
        double** timestamps_arrays = <double**>malloc(num_series * sizeof(double*))
        double** values_arrays = <double**>malloc(num_series * sizeof(double*))
        int* series_lengths = <int*>malloc(num_series * sizeof(int))
        double* result_timestamps = NULL
        double* result_values = NULL
        int result_count

    if not timestamps_arrays or not values_arrays or not series_lengths:
        # Memory allocation failed
        if timestamps_arrays:
            free(timestamps_arrays)
        if values_arrays:
            free(values_arrays)
        if series_lengths:
            free(series_lengths)
        raise MemoryError("Failed to allocate memory for merge operation")

    # Initialize pointers to NULL for safe cleanup
    for i in range(num_series):
        timestamps_arrays[i] = NULL
        values_arrays[i] = NULL

    try:
        # Extract all data from Python objects into C arrays
        for i in range(num_series):
            series = active_series[i]
            series_lengths[i] = len(series)
            total_points += series_lengths[i]

            timestamps_arrays[i] = <double*>malloc(series_lengths[i] * sizeof(double))
            values_arrays[i] = <double*>malloc(series_lengths[i] * sizeof(double))

            if not timestamps_arrays[i] or not values_arrays[i]:
                alloc_failed = True
                break

            # Copy data from Python objects to C arrays
            for j in range(series_lengths[i]):
                point = series[j]
                timestamps_arrays[i][j] = point.timestamp
                values_arrays[i][j] = point.value

        if alloc_failed:
            raise MemoryError("Failed to allocate memory for series data")

        # Perform merge with full nogil
        # Pass total_points as capacity (worst case: all points output)
        with nogil:
            result_count = _kway_merge_timeseries_nogil(timestamps_arrays, values_arrays,
                                               series_lengths, num_series,
                                               total_points,
                                               &result_timestamps, &result_values)

        if result_count < 0:
            # Note: _kway_merge_timeseries_nogil frees all memory on error
            raise MemoryError("Failed during merge operation")

        # Convert C arrays back to Python tuples
        merged = [None] * result_count
        for i in range(result_count):
            merged[i] = (result_timestamps[i], result_values[i])

        return merged

    finally:
        # Centralized cleanup: safe even if some allocations failed
        # Free result arrays (allocated by _kway_merge_timeseries_nogil)
        if result_timestamps:
            free(result_timestamps)
        if result_values:
            free(result_values)
        if timestamps_arrays:
            for i in range(num_series):
                if timestamps_arrays[i]:
                    free(timestamps_arrays[i])
            free(timestamps_arrays)
        if values_arrays:
            for i in range(num_series):
                if values_arrays[i]:
                    free(values_arrays[i])
            free(values_arrays)
        if series_lengths:
            free(series_lengths)


cdef double _compute_time_weighted_average_nogil(double* timestamps, double* values, int n,
                                                 double window_start, double window_end) noexcept nogil:
    """
    Fully nogil time-weighted average computation on C arrays.

    Returns: Time-weighted average or NaN to indicate None (invalid result)
    """
    cdef:
        int i
        double total_weighted_value = 0.0
        double total_duration = 0.0
        double current_value = 0.0
        double current_time
        double timestamp, value, duration

    if window_end <= window_start:
        return nan("")

    current_time = window_start

    # Find value at window_start (LOCF)
    for i in range(n):
        timestamp = timestamps[i]

        if timestamp <= window_start:
            current_value = values[i]
        else:
            break

    # Process segments
    for i in range(n):
        timestamp = timestamps[i]
        value = values[i]

        if timestamp <= window_start:
            continue

        if timestamp >= window_end:
            break

        # Add contribution of current segment
        # Note: timestamp < window_end is guaranteed here due to the break above
        duration = timestamp - current_time

        if duration > 0:
            total_weighted_value += current_value * duration
            total_duration += duration

        current_value = value
        current_time = timestamp

    # Add final segment
    if current_time < window_end:
        duration = window_end - current_time
        total_weighted_value += current_value * duration
        total_duration += duration

    if total_duration > 0:
        return total_weighted_value / total_duration

    return nan("")


def time_weighted_average_cython(list timeseries, double window_start,
                                  double window_end, double last_window_s=1.0):
    """
    Cython-optimized time-weighted average calculation.

    Assumptions:
        - Input timeseries is sorted by timestamp in ascending order
        - Values are treated as a step function (LOCF - Last Observation Carried Forward)

    Args:
        timeseries: List of objects with .timestamp and .value attributes
        window_start: Start of window (negative infinity means use first timestamp)
        window_end: End of window (negative infinity means use last timestamp + last_window_s)
        last_window_s: Window size for last segment

    Returns:
        Time-weighted average or None (returned as None when result would be invalid)
    """
    if not timeseries:
        return None

    cdef:
        int n = len(timeseries)
        int i
        double result
        object point
        double* timestamps = <double*>malloc(n * sizeof(double))
        double* values = <double*>malloc(n * sizeof(double))

    if not timestamps or not values:
        if timestamps:
            free(timestamps)
        if values:
            free(values)
        raise MemoryError("Failed to allocate memory for time weighted average")

    try:
        # Extract data from Python objects into C arrays
        for i in range(n):
            point = timeseries[i]
            timestamps[i] = point.timestamp
            values[i] = point.value

        # Handle window boundaries
        # Use negative infinity as sentinel for None (any valid float including -1.0 works)
        if isinf(window_start) and window_start < 0:
            window_start = timestamps[0]

        if isinf(window_end) and window_end < 0:
            window_end = timestamps[n - 1] + last_window_s

        # Compute with full nogil
        with nogil:
            result = _compute_time_weighted_average_nogil(timestamps, values, n,
                                                          window_start, window_end)

        return None if isnan(result) else result

    finally:
        free(timestamps)
        free(values)

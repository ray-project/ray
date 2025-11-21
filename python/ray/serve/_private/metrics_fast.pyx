# cython: profile=False
# cython: embedsignature = True
# cython: language_level = 3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False
# distutils: language = c++

"""
Cython-optimized implementations of performance-critical metrics functions.

This module provides native-code implementations of hot path functions used
in Ray Serve autoscaling, particularly the k-way merge algorithm for timeseries.
"""

# Python imports
from ray.serve._private.common import TimeStampedValue

# C library imports
from libc.stdlib cimport malloc, free
from libc.math cimport round as c_round

# Heap node for k-way merge
cdef struct HeapNode:
    double timestamp
    int replica_idx
    double value
    int series_idx  # Current position in the series


cdef void heap_sift_down(HeapNode* heap, int size, int pos) nogil:
    """Sift down operation for min-heap (inline for performance)."""
    cdef int smallest = pos
    cdef int left = 2 * pos + 1
    cdef int right = 2 * pos + 2
    cdef HeapNode temp

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


cdef void heap_sift_up(HeapNode* heap, int pos) nogil:
    """Sift up operation for min-heap (inline for performance)."""
    cdef int parent
    cdef HeapNode temp

    while pos > 0:
        parent = (pos - 1) // 2
        if heap[parent].timestamp <= heap[pos].timestamp:
            break

        # Swap
        temp = heap[pos]
        heap[pos] = heap[parent]
        heap[parent] = temp
        pos = parent


cdef void heap_pop(HeapNode* heap, int* size) nogil:
    """Remove minimum element from heap."""
    if size[0] <= 0:
        return

    heap[0] = heap[size[0] - 1]
    size[0] -= 1
    if size[0] > 0:
        heap_sift_down(heap, size[0], 0)


cdef void heap_push(HeapNode* heap, int* size, HeapNode node) nogil:
    """Add element to heap."""
    heap[size[0]] = node
    heap_sift_up(heap, size[0])
    size[0] += 1


def merge_instantaneous_total_cython(list replicas_timeseries):
    """
    Cython-optimized k-way merge for timeseries.

    This is a drop-in replacement for the Python version with 5-10x speedup.

    Args:
        replicas_timeseries: List of TimeSeries (List[List[TimeStampedValue]])

    Returns:
        Merged TimeSeries
    """
    # Filter empty series
    cdef list active_series = [series for series in replicas_timeseries if series]

    if not active_series:
        return []

    if len(active_series) == 1:
        # Performance optimization: return single series directly
        return active_series[0]

    cdef:
        int num_series = len(active_series)
        int i, series_idx, replica_idx
        int heap_size = 0
        double timestamp, value, old_value
        double running_total = 0.0
        double rounded_timestamp, last_rounded_timestamp = -1.0
        object point, series
        HeapNode new_node
        # C arrays for performance
        double* current_values = <double*>malloc(num_series * sizeof(double))
        int* series_positions = <int*>malloc(num_series * sizeof(int))
        int* series_lengths = <int*>malloc(num_series * sizeof(int))
        HeapNode* merge_heap = <HeapNode*>malloc(num_series * sizeof(HeapNode))

    if not current_values or not series_positions or not series_lengths or not merge_heap:
        # Memory allocation failed
        if current_values:
            free(current_values)
        if series_positions:
            free(series_positions)
        if series_lengths:
            free(series_lengths)
        if merge_heap:
            free(merge_heap)
        raise MemoryError("Failed to allocate memory for merge operation")

    try:
        # Initialize arrays
        for i in range(num_series):
            current_values[i] = 0.0
            series_positions[i] = 0
            series = active_series[i]
            series_lengths[i] = len(series)

            # Push first element from each series to heap
            if series_lengths[i] > 0:
                point = series[0]
                merge_heap[heap_size].timestamp = point.timestamp
                merge_heap[heap_size].replica_idx = i
                merge_heap[heap_size].value = point.value
                merge_heap[heap_size].series_idx = 0
                heap_size += 1

        # Build initial heap
        for i in range(heap_size // 2 - 1, -1, -1):
            heap_sift_down(merge_heap, heap_size, i)

        # Result list
        merged = []

        # K-way merge
        while heap_size > 0:
            # Get minimum element
            timestamp = merge_heap[0].timestamp
            replica_idx = merge_heap[0].replica_idx
            value = merge_heap[0].value
            series_idx = merge_heap[0].series_idx

            # Update running total
            old_value = current_values[replica_idx]
            current_values[replica_idx] = value
            running_total += value - old_value

            # Remove from heap
            heap_pop(merge_heap, &heap_size)

            # Push next element from same series if available
            series_positions[replica_idx] = series_idx + 1
            if series_positions[replica_idx] < series_lengths[replica_idx]:
                series = active_series[replica_idx]
                point = series[series_positions[replica_idx]]

                new_node.timestamp = point.timestamp
                new_node.replica_idx = replica_idx
                new_node.value = point.value
                new_node.series_idx = series_positions[replica_idx]

                heap_push(merge_heap, &heap_size, new_node)

            # Only add point if value changed
            if value != old_value:
                # Round to 10ms precision
                rounded_timestamp = c_round(timestamp * 100.0) / 100.0

                # Check if we can merge with last point
                if len(merged) > 0 and last_rounded_timestamp == rounded_timestamp:
                    # Update last point's value (use explicit index instead of -1)
                    merged[len(merged) - 1] = TimeStampedValue(rounded_timestamp, running_total)
                else:
                    # Add new point
                    merged.append(TimeStampedValue(rounded_timestamp, running_total))
                    last_rounded_timestamp = rounded_timestamp

        return merged

    finally:
        # Clean up
        free(current_values)
        free(series_positions)
        free(series_lengths)
        free(merge_heap)


def time_weighted_average_cython(list timeseries, double window_start=-1.0,
                                  double window_end=-1.0, double last_window_s=1.0):
    """
    Cython-optimized time-weighted average calculation.

    Args:
        timeseries: List of TimeStampedValue objects
        window_start: Start of window (-1.0 means use first timestamp)
        window_end: End of window (-1.0 means use last timestamp + last_window_s)
        last_window_s: Window size for last segment

    Returns:
        Time-weighted average or None
    """
    if not timeseries:
        return None

    cdef:
        int n = len(timeseries)
        int i
        double total_weighted_value = 0.0
        double total_duration = 0.0
        double current_value = 0.0
        double current_time
        double timestamp, value, segment_end, duration
        object point

    # Handle window boundaries
    if window_start < 0:
        point = timeseries[0]
        window_start = point.timestamp

    if window_end < 0:
        point = timeseries[n - 1]  # Use explicit index instead of -1
        window_end = point.timestamp + last_window_s

    if window_end <= window_start:
        return None

    current_time = window_start

    # Find value at window_start (LOCF)
    for i in range(n):
        point = timeseries[i]
        timestamp = point.timestamp

        if timestamp <= window_start:
            current_value = point.value
        else:
            break

    # Process segments
    for i in range(n):
        point = timeseries[i]
        timestamp = point.timestamp
        value = point.value

        if timestamp <= window_start:
            continue

        if timestamp >= window_end:
            break

        # Add contribution of current segment
        segment_end = timestamp if timestamp < window_end else window_end
        duration = segment_end - current_time

        if duration > 0:
            total_weighted_value += current_value * duration
            total_duration += duration

        current_value = value
        current_time = segment_end

    # Add final segment
    if current_time < window_end:
        duration = window_end - current_time
        total_weighted_value += current_value * duration
        total_duration += duration

    if total_duration > 0:
        return total_weighted_value / total_duration

    return None

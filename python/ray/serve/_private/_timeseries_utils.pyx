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
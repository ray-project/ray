#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <queue>
#include <vector>

#include "csortlib.h"

namespace csortlib {

template <typename T>
size_t _TotalSize(const std::vector<T>& parts) {
    size_t ret = 0;
    for (const auto& part : parts) {
        ret += part.size;
    }
    return ret;
}

#ifdef CSORTLIB_USE_ALT_MEMORY_LAYOUT

// 16-byte word-aligned data structure for sorting.
// Significantly more cache-friendly.
struct SortItem {
    uint8_t header[HEADER_SIZE];
    Record* ptr;
    uint8_t unused[2];
};

struct SortItemComparator {
    inline bool operator()(const SortItem& a, const SortItem& b) {
        return std::memcmp(a.header, b.header, HEADER_SIZE) < 0;
    }
};

std::unique_ptr<std::vector<SortItem>> _MakeSortItems(
    const Array<Record>& record_array) {
    Record* const records = record_array.ptr;
    const size_t num_records = record_array.size;
    auto ret = std::make_unique<std::vector<SortItem>>();
    ret->reserve(num_records);
    SortItem item;
    for (Record* ptr = records; ptr != records + num_records; ++ptr) {
        memcpy(item.header, ptr->header, HEADER_SIZE);
        item.ptr = ptr;
        ret->emplace_back(item);
    }
    return ret;
}

#endif

std::vector<Partition> SortAndPartition(const Array<Record>& record_array,
                                        const std::vector<Key>& boundaries) {
    Record* const records = record_array.ptr;
    const size_t num_records = record_array.size;

#ifdef CSORTLIB_USE_ALT_MEMORY_LAYOUT
    auto sort_items = _MakeSortItems(record_array);

    const auto start1 = std::chrono::high_resolution_clock::now();

    std::sort(sort_items->begin(), sort_items->end(),
              HeaderComparator<SortItem>());

    const auto stop1 = std::chrono::high_resolution_clock::now();
    printf("Sort,%ld\n",
           std::chrono::duration_cast<std::chrono::milliseconds>(stop1 - start1)
               .count());

    Record* buffer = new Record[num_records];
    Record* cur = buffer;
    for (const auto& item : *sort_items) {
        memcpy(cur++, item.ptr, sizeof(Record));
    }
    memcpy(records, buffer, sizeof(Record) * num_records);
    delete[] buffer;
#else
    std::sort(records, records + num_records, HeaderComparator<Record>());
#endif

    std::vector<Partition> ret;
    ret.reserve(boundaries.size());
    auto bound = boundaries.begin();
    size_t off = 0;
    size_t prev_off = 0;
    while (off < num_records && bound != boundaries.end()) {
        while (records[off].key() < *bound) {
            ++off;
        }
        const size_t size = off - prev_off;
        if (!ret.empty()) {
            ret.back().size = size;
        }
        ret.emplace_back(Partition{off, 0});
        ++bound;
        prev_off = off;
    }
    if (!ret.empty()) {
        ret.back().size = num_records - prev_off;
    }
    assert(ret.size() == boundaries.size());
    assert(_TotalSize(ret) == num_records);
    return ret;
}

std::vector<Key> GetBoundaries(size_t num_partitions) {
    std::vector<Key> ret;
    ret.reserve(num_partitions);
    const Key min = 0;
    const Key max = std::numeric_limits<Key>::max();
    const Key step = ceil(max / num_partitions);
    Key boundary = min;
    for (size_t i = 0; i < num_partitions; ++i) {
        ret.emplace_back(boundary);
        boundary += step;
    }
    return ret;
}

struct SortData {
    const Record* record;
    size_t partition;
    size_t index;
};

struct SortDataComparator {
    bool operator()(const SortData& a, const SortData& b) {
        return !HeaderComparator<Record>()(*a.record, *b.record);
    }
};

class Merger::Impl {
   public:
    Impl(const std::vector<ConstArray<Record>>& parts) : parts_(parts) {
        for (size_t i = 0; i < parts_.size(); ++i) {
            if (parts_[i].size > 0) {
                heap_.push({parts_[i].ptr, i, 0});
            }
        }
    }

    size_t GetBatch(Record* const& ret, size_t max_num_records) {
        size_t cnt = 0;
        auto cur = ret;
        while (!heap_.empty()) {
            if (cnt >= max_num_records) {
                return cnt;
            }
            const SortData top = heap_.top();
            heap_.pop();
            const size_t i = top.partition;
            const size_t j = top.index;
            // Copy record to output array
            *cur++ = parts_[i].ptr[j];
            ++cnt;
            if (j + 1 < parts_[i].size) {
                heap_.push({parts_[i].ptr + j + 1, i, j + 1});
            }
        }
        return cnt;
    }

   private:
    const std::vector<ConstArray<Record>> parts_;
    std::priority_queue<SortData, std::vector<SortData>, SortDataComparator>
        heap_;
};

Merger::Merger(const std::vector<ConstArray<Record>>& parts)
    : impl_(std::make_unique<Impl>(parts)) {}

size_t Merger::GetBatch(Record* const& ret, size_t max_num_records) {
    return impl_->GetBatch(ret, max_num_records);
}

Array<Record> MergePartitions(const std::vector<ConstArray<Record>>& parts) {
    const size_t num_records = _TotalSize(parts);
    if (num_records == 0) {
        return {nullptr, 0};
    }
    Record* const ret = new Record[num_records];
    Merger merger(parts);
    merger.GetBatch(ret, num_records);
    return {ret, num_records};
}

}  // namespace csortlib

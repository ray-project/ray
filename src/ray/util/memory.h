#ifndef RAY_UTIL_MEMORY_H
#define RAY_UTIL_MEMORY_H

#include <stdint.h>

namespace ray {

// A helper function for doing memcpy with multiple threads. This is required
// to saturate the memory bandwidth of modern cpus.
void parallel_memcopy(uint8_t *dst, const uint8_t *src, int64_t nbytes,
                      uintptr_t block_size, int num_threads);

}  // namespace ray

#endif  // RAY_UTIL_MEMORY_H

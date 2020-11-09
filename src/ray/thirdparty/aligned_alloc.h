// Adopted from https://github.com/NickStrupat/AlignedMalloc

#ifndef ALIGNED_ALLOC_H_INCLUDED
#define ALIGNED_ALLOC_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

void * aligned_malloc(size_t size, size_t alignment);
void aligned_free(void * pointer);

#ifdef __cplusplus
}
#endif

#endif

#include "ray/thirdparty/aligned_alloc.h"

#if defined(__APPLE__) || defined(__linux__)

#include <stdlib.h>

void *aligned_malloc(size_t size, size_t alignment) {
	void *pointer;
	posix_memalign(&pointer, alignment, size);
	return pointer;
}

void aligned_free(void *pointer) {
	free(pointer);
}

#elif defined(_WIN32)

#include <malloc.h>

void *aligned_malloc(size_t size, size_t alignment) {
	return _aligned_malloc(size, alignment);
}

void aligned_free(void *pointer) {
	_aligned_free(pointer);
}

#else

// https://sites.google.com/site/ruslancray/lab/bookshelf/interview/ci/low-level/write-an-aligned-malloc-free-function
#include <stdlib.h>

void *aligned_malloc(size_t size, size_t alignment) {
	void *p1; // original block
	void **p2; // aligned block
	int offset = alignment - 1 + sizeof(void *);
	if ((p1 = (void *)malloc(size + offset)) == NULL)
		return NULL;
	p2 = (void **)(((size_t)(p1) + offset) & ~(alignment - 1));
	p2[-1] = p1;
	return p2;
}

void aligned_free(void *pointer) {
	free(((void **)pointer)[-1]);
}

#endif

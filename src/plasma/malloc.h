#ifndef MALLOC_H
#define MALLOC_H

#ifdef __cplusplus
extern "C" {
#endif

void get_malloc_mapinfo(void *addr,
                        int *fd,
                        int64_t *map_length,
                        ptrdiff_t *offset);

#ifdef __cplusplus
}
#endif

#endif /* MALLOC_H */

// src/ray/core_worker/lib/go/gcs_memory.cc
#include "gcs_memory.h"
#include <cstdlib>

extern "C" {
    void ray_gcs_free_memory(void* ptr) {
        if (ptr) {
            free(ptr);
        }
    }

    void ray_gcs_free_string_array(char** arr, int count) {
        if (!arr) return;
        for (int i = 0; i < count; i++) {
            if (arr[i]) {
                ray_gcs_free_memory(arr[i]);
            }
        }
        ray_gcs_free_memory(arr);
    }
}

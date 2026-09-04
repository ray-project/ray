// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// src/ray/core_worker/lib/go/gcs_client_utils.cc
// CGO bridge helper implementations
#include "gcs_client_utils.h"
#include <cstdlib>
#include <cstring>

// Helper: set an error message
void set_error(char** error_out, const char* msg) {
    if (error_out && msg) {
        size_t len = strlen(msg);
        *error_out = static_cast<char*>(malloc(len + 1));
        if (*error_out) {
            strcpy(*error_out, msg);
        }
    }
}

// Helper: allocate a serialized string array (with sizes)
bool allocate_serialized_array(
    const std::vector<std::string>& data,
    char*** serialized_out,
    int** sizes_out,
    int* count_out) {

    int result_count = static_cast<int>(data.size());
    *count_out = result_count;

    if (result_count == 0) {
        *serialized_out = nullptr;
        *sizes_out = nullptr;
        return true;
    }

    *serialized_out = static_cast<char**>(malloc(result_count * sizeof(char*)));
    *sizes_out = static_cast<int*>(malloc(result_count * sizeof(int)));

    if (!*serialized_out || !*sizes_out) {
        if (*serialized_out) free(*serialized_out);
        if (*sizes_out) free(*sizes_out);
        *serialized_out = nullptr;
        *sizes_out = nullptr;
        return false;
    }

    for (int i = 0; i < result_count; i++) {
        size_t data_size = data[i].size();
        (*serialized_out)[i] = static_cast<char*>(malloc(data_size));
        (*sizes_out)[i] = static_cast<int>(data_size);

        if (!(*serialized_out)[i]) {
            for (int j = 0; j < i; j++) {
                free((*serialized_out)[j]);
            }
            free(*serialized_out);
            free(*sizes_out);
            *serialized_out = nullptr;
            *sizes_out = nullptr;
            return false;
        }
        memcpy((*serialized_out)[i], data[i].c_str(), data_size);
    }

    return true;
}

// Helper: allocate a string array (without sizes, for string IDs)
bool allocate_string_array(
    const std::vector<std::string>& data,
    char*** string_out,
    int* count_out) {

    int result_count = static_cast<int>(data.size());
    *count_out = result_count;

    if (result_count == 0) {
        *string_out = nullptr;
        return true;
    }

    *string_out = static_cast<char**>(malloc(result_count * sizeof(char*)));
    if (!*string_out) {
        return false;
    }

    for (int i = 0; i < result_count; i++) {
        size_t str_len = data[i].size();
        (*string_out)[i] = static_cast<char*>(malloc(str_len + 1));
        if (!(*string_out)[i]) {
            for (int j = 0; j < i; j++) {
                free((*string_out)[j]);
            }
            free(*string_out);
            *string_out = nullptr;
            return false;
        }
        strcpy((*string_out)[i], data[i].c_str());
    }

    return true;
}

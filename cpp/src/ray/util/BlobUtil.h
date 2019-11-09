#pragma once

#include <ray/api/Blob.h>

namespace ray {
::ray::blob blob_merge(std::vector< ::ray::blob> &&blobs);

::ray::blob blob_merge(const std::vector< ::ray::blob> &blobs);

std::unique_ptr< ::ray::blob> blob_merge_to_ptr(const std::vector< ::ray::blob> &&blobs);

std::unique_ptr< ::ray::blob> blob_merge_to_ptr(const std::vector< ::ray::blob> &blobs);
}
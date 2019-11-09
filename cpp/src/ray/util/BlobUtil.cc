
#include "BlobUtil.h"

namespace ray {
::ray::blob blob_merge(std::vector< ::ray::blob> &&blobs) {
  size_t total_size = 0;
  for (int i = 0; i < static_cast<int>(blobs.size()); i++) {
    total_size += blobs[i].length();
  }

  std::shared_ptr<char> bptr(::ray::make_shared_array<char>(total_size));
  ::ray::blob bb(bptr, total_size);
  const char *ptr = bb.data();

  for (int i = 0; i < static_cast<int>(blobs.size()); i++) {
    memcpy((void *)ptr, (const void *)blobs[i].data(), (size_t)blobs[i].length());
    ptr += blobs[i].length();
  }
  return bb;
}

::ray::blob blob_merge(const std::vector< ::ray::blob> &blobs) {
  size_t total_size = 0;
  for (int i = 0; i < static_cast<int>(blobs.size()); i++) {
    total_size += blobs[i].length();
  }
  std::shared_ptr<char> bptr(::ray::make_shared_array<char>(total_size));
  ::ray::blob bb(bptr, total_size);
  const char *ptr = bb.data();

  for (int i = 0; i < static_cast<int>(blobs.size()); i++) {
    memcpy((void *)ptr, (const void *)blobs[i].data(), (size_t)blobs[i].length());
    ptr += blobs[i].length();
  }
  return bb;
}

std::unique_ptr< ::ray::blob> blob_merge_to_ptr(const std::vector< ::ray::blob> &&blobs) {
  size_t total_size = 0;
  for (int i = 0; i < static_cast<int>(blobs.size()); i++) {
    total_size += blobs[i].length();
  }
  std::shared_ptr<char> bptr(::ray::make_shared_array<char>(total_size));
  std::unique_ptr< ::ray::blob> bb(new ::ray::blob(bptr, total_size));
  const char *ptr = bb->data();

  for (int i = 0; i < static_cast<int>(blobs.size()); i++) {
    memcpy((void *)ptr, (const void *)blobs[i].data(), (size_t)blobs[i].length());
    ptr += blobs[i].length();
  }
  return bb;
}

std::unique_ptr< ::ray::blob> blob_merge_to_ptr(const std::vector< ::ray::blob> &blobs) {
  size_t total_size = 0;
  for (int i = 0; i < static_cast<int>(blobs.size()); i++) {
    total_size += blobs[i].length();
  }
  std::shared_ptr<char> bptr(::ray::make_shared_array<char>(total_size));
  std::unique_ptr< ::ray::blob> bb(new ::ray::blob(bptr, total_size));
  const char *ptr = bb->data();

  for (int i = 0; i < static_cast<int>(blobs.size()); i++) {
    memcpy((void *)ptr, (const void *)blobs[i].data(), (size_t)blobs[i].length());
    ptr += blobs[i].length();
  }
  return bb;
}
}
#ifndef RAY_RAYLET_GCS_FORMAT_UTIL_H
#define RAY_RAYLET_GCS_FORMAT_UTIL_H

#include "ray/gcs/format/gcs_generated.h"

namespace std {

template <>
struct hash<Language> {
  size_t operator()(const Language &language) const {
    return std::hash<int32_t>()(static_cast<int32_t>(language));
  }
};

template <>
struct hash<const Language> {
  size_t operator()(const Language &language) const {
    return std::hash<int32_t>()(static_cast<int32_t>(language));
  }
};

}  // namespace std

#endif  // RAY_RAYLET_GCS_FORMAT_UTIL_H

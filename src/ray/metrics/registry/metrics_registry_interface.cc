#include "metrics_registry_interface.h"

#include <math.h>

namespace ray {

namespace metrics {

std::vector<double> MetricsRegistryInterface::GenBucketBoundaries(
    double min_value, double max_value, size_t bucket_count) const {
  std::vector<double> boundaries;
  // min_value should be always less or equal than max_value
  if (min_value >= max_value) {
    boundaries.emplace_back(min_value);
    return boundaries;
  }

  if (bucket_count == 0) {
    bucket_count = 2;
  }

  double diff = max_value - min_value;
  double bucket_range = diff / bucket_count;
  double cur_boundary = min_value;
  while (cur_boundary < max_value) {
    boundaries.emplace_back(cur_boundary);
    cur_boundary += bucket_range;
  }
  boundaries.emplace_back(max_value);
  return boundaries;
}

}  // namespace metrics

}  // namespace ray

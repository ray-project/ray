// Copyright 2026 The Ray Authors.
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
//
// This implementation is based on tehuti's Percentiles class:
// https://github.com/tehuti-io/tehuti
// Original implementation: Copyright LinkedIn Corp. under Apache License 2.0

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

namespace ray {
namespace stats {

/**
  @class LinearBinScheme

  Binning scheme where bins grow quadratically to provide better precision for smaller
  values. This is particularly useful for latency distributions where most values are
  small but occasional spikes can be large.

  Bin boundaries follow the formula: bin_value = scale * b * (b + 1) / 2
  Bin widths grow linearly: 1x, 2x, 3x, 4x...

  This provides fine granularity where most data is (low latencies) and coarse
  granularity for rare outliers (high latencies).
 */
class LinearBinScheme {
 public:
  /**
    Create a linear bin scheme.

    @param num_bins Number of bins to create.
    @param max_value Maximum expected value for binning.
   */
  LinearBinScheme(int num_bins, double max_value)
      : num_bins_(num_bins), max_value_(max_value) {
    scale_ = max_value / (num_bins * (num_bins - 1) / 2.0);
  }

  /**
    Get the total number of bins.

    @return Number of bins.
   */
  int NumBins() const { return num_bins_; }

  /**
    Map a value to its corresponding bin index.

    @param value The value to map to a bin.
    @return The bin index for this value.
   */
  int ToBin(double value) const {
    if (value < 0.0) {
      return 0;
    } else if (value > max_value_) {
      return num_bins_ - 1;
    } else {
      double scaled = value / scale_;
      // Solve quadratic: b * (b + 1) / 2 = scaled
      return static_cast<int>(-0.5 + std::sqrt(2.0 * scaled + 0.25));
    }
  }

  /**
    Get the representative value for a bin index.

    @param bin The bin index.
    @return The representative value for this bin.
   */
  double FromBin(int bin) const {
    if (bin >= num_bins_ - 1) {
      return std::numeric_limits<double>::infinity();
    } else {
      double unscaled = (bin * (bin + 1.0)) / 2.0;
      return unscaled * scale_;
    }
  }

 private:
  int num_bins_;
  double max_value_;
  double scale_;
};

/**
  @class PercentileHistogram

  Lightweight histogram for percentile tracking using bin-based approximation.
 */
class PercentileHistogram {
 public:
  /**
    Create a histogram with the specified binning scheme.

    @param bin_scheme The binning scheme to use. Must outlive this histogram.
   */
  explicit PercentileHistogram(const LinearBinScheme *bin_scheme)
      : bin_scheme_(bin_scheme), counts_(bin_scheme->NumBins(), 0.0f), count_(0) {}

  /**
    Record a value in the histogram.

    @param value The value to record.
   */
  void Record(double value) {
    int bin = bin_scheme_->ToBin(value);
    counts_[bin] += 1.0f;
    count_ += 1;
  }

  /**
    Clear all recorded values.
   */
  void Clear() {
    std::fill(counts_.begin(), counts_.end(), 0.0f);
    count_ = 0;
  }

  /**
    Calculate a percentile from the histogram.

    @param quantile The desired quantile (0.0 to 1.0). For example, 0.95 for P95.
    @return The approximate percentile value, or NaN if no data has been recorded.
   */
  double GetPercentile(double quantile) const {
    if (count_ == 0) {
      return std::nan("");
    }
    float sum = 0.0f;
    for (size_t i = 0; i < counts_.size(); i++) {
      sum += counts_[i];
      if (sum / count_ > quantile) {
        return bin_scheme_->FromBin(i);
      }
    }
    return std::numeric_limits<double>::infinity();
  }

  /**
    Get the maximum value observed (approximated by highest non-empty bin).

    @return The approximate maximum value, or NaN if no data has been recorded.
   */
  double GetMax() const {
    if (count_ == 0) {
      return std::nan("");
    }
    for (int i = counts_.size() - 1; i >= 0; i--) {
      if (counts_[i] > 0) {
        return bin_scheme_->FromBin(i);
      }
    }
    return std::nan("");
  }

  /**
    Get the mean value (approximated using bin midpoints).

    @return The approximate mean value, or NaN if no data has been recorded.
   */
  double GetMean() const {
    if (count_ == 0) {
      return std::nan("");
    }
    double sum = 0.0;
    for (size_t i = 0; i < counts_.size(); i++) {
      if (counts_[i] > 0) {
        double bin_value = bin_scheme_->FromBin(i);
        if (std::isfinite(bin_value)) {
          sum += bin_value * counts_[i];
        }
      }
    }
    return sum / count_;
  }

  /**
    Get the total number of values recorded.

    @return The count of recorded values.
   */
  int64_t GetCount() const { return count_; }

  /**
    Get the raw bin counts.

    @return Vector of counts for each bin.
   */
  const std::vector<float> &GetCounts() const { return counts_; }

 private:
  const LinearBinScheme *bin_scheme_;
  std::vector<float> counts_;
  int64_t count_;
};

/**
  @class PercentileTracker

  Tracks latency percentiles using histogram-based approximation. This is a
  memory-efficient alternative to storing all raw values.

  IMPORTANT - AGGREGATION WARNING:
  Percentile values from this tracker CANNOT be meaningfully aggregated across nodes.
  DO NOT average, sum, or otherwise combine P50/P95/P99 values from multiple trackers.

  Why? Percentiles are order statistics based on rank position in the data distribution.
  Aggregating percentiles loses critical information about sample sizes and underlying
  distributions, leading to incorrect results.

  Example: If Node A has P95=10ms (1000 samples) and Node B has P95=1900ms (10 samples),
  averaging gives 955ms, but the true combined P95 is ~10ms.

  If you need cluster-wide percentiles, either:
  1. Aggregate histograms (with same bucket boundaries) then recalculate percentiles
  2. Use mergeable sketches like T-Digest
  3. Collect raw data centrally and calculate percentiles there

  This tracker is designed for per-node observability, not cross-node aggregation.
 */
class PercentileTracker {
 public:
  /**
    Create a percentile tracker optimized for latency measurements.

    Uses linear binning which provides better precision for smaller values,
    which is typical for latency distributions (long-tail).

    @param size_in_bytes Approximate memory usage in bytes (e.g., 1024 bytes = 256 bins).
    @param max_value Maximum expected value for bin range.
    @return A PercentileTracker configured for latency tracking.
   */
  static PercentileTracker Create(int size_in_bytes, double max_value) {
    int num_bins = size_in_bytes / sizeof(float);
    return PercentileTracker(num_bins, max_value);
  }

  /**
    Record a latency value.

    @param value The latency value to record (e.g., milliseconds).
   */
  void Record(double value) { histogram_->Record(value); }

  /**
    Clear all recorded values.
   */
  void Clear() { histogram_->Clear(); }

  /**
    Get the 50th percentile (median).

    @return P50 value, or NaN if no data recorded.
   */
  double GetP50() const { return histogram_->GetPercentile(0.50); }

  /**
    Get the 95th percentile.

    @return P95 value, or NaN if no data recorded.
   */
  double GetP95() const { return histogram_->GetPercentile(0.95); }

  /**
    Get the 99th percentile.

    @return P99 value, or NaN if no data recorded.
   */
  double GetP99() const { return histogram_->GetPercentile(0.99); }

  /**
    Get the maximum observed value (approximated).

    @return Max value, or NaN if no data recorded.
   */
  double GetMax() const { return histogram_->GetMax(); }

  /**
    Get the mean value (approximated).

    @return Mean value, or NaN if no data recorded.
   */
  double GetMean() const { return histogram_->GetMean(); }

  /**
    Get the total number of values recorded.

    @return Count of recorded values.
   */
  int64_t GetCount() const { return histogram_->GetCount(); }

  /**
    Swap the current histogram with a new empty one and return the old histogram.

    This enables lock-free percentile calculation: swap out the histogram under a lock
    (fast), then calculate percentiles from the old histogram without holding the lock.

    @return The old histogram containing accumulated data.
   */
  std::unique_ptr<PercentileHistogram> SwapHistogram() {
    auto old_histogram = std::move(histogram_);
    histogram_ = std::make_unique<PercentileHistogram>(bin_scheme_.get());
    return old_histogram;
  }

 private:
  PercentileTracker(int num_bins, double max_value)
      : bin_scheme_(std::make_shared<LinearBinScheme>(num_bins, max_value)),
        histogram_(std::make_unique<PercentileHistogram>(bin_scheme_.get())) {}

  // Store bin_scheme in shared_ptr to avoid dangling pointers when PercentileTracker is
  // moved
  std::shared_ptr<LinearBinScheme> bin_scheme_;
  std::unique_ptr<PercentileHistogram> histogram_;
};

}  // namespace stats
}  // namespace ray

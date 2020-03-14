// Copyright 2017 The Ray Authors.
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

#ifndef RAY_GCS_DETECTOR_H
#define RAY_GCS_DETECTOR_H

#include <boost/asio.hpp>

namespace ray {

namespace gcs {
class RedisGcsClient;

/// GcsDetector is responsible for monitoring redis.
class GcsDetector {
 public:
  /// Create a GcsDetector.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param gcs_client The client of gcs to access/pub/sub data.
  explicit GcsDetector(boost::asio::io_service &io_service,
                       std::shared_ptr<gcs::RedisGcsClient> gcs_client,
                       std::function<void()> destroy_callback);

 protected:
  /// Start to detect gcs.
  void Start();

  /// A periodic timer that fires on every gcs detect period.
  void Tick();

  /// Schedule another tick after a short time.
  void ScheduleTick();

  /// Check that if redis is inactive.
  void DetectRedis();

 private:
  /// A client to the GCS, through which ping redis.
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_;

  /// A timer that ticks every gcs_detect_timeout_milliseconds.
  boost::asio::deadline_timer detect_timer_;

  /// A function is called when redis is detected to be unavailable.
  std::function<void()> callback_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_DETECTOR_H

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

#pragma once

#include <spdlog/sinks/base_sink.h>

#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/str_split.h"
#include "ray/util/compat.h"

namespace ray {

// A sink which streams only after a newliner or explicit flush.
template <typename Mutex>
class spdlog_newliner_sink final : public spdlog::sinks::base_sink<Mutex> {
 public:
  explicit spdlog_newliner_sink(spdlog::sink_ptr internal_sink)
      : internal_sink_(std::move(internal_sink)) {}

 protected:
  void sink_it_(const spdlog::details::log_msg &msg) override {
    if (msg.payload.size() == 0) {
      return;
    }

    const std::string_view new_content{msg.payload.data(), msg.payload.size()};
    auto pos = new_content.find('\n');
    if (pos == std::string_view::npos) {
      buffer_ += new_content;
      return;
    }
    std::vector<std::string_view> segments = absl::StrSplit(new_content, '\n');
    for (int idx = 0; idx < static_cast<int>(segments.size()) - 1; ++idx) {
      std::string cur_message = std::move(buffer_);
      buffer_.clear();
      cur_message += segments[idx];
      // Compensate the missing newliner we miss when split.
      cur_message += '\n';

      spdlog::details::log_msg new_log_msg;
      new_log_msg.payload = std::string_view{cur_message.data(), cur_message.length()};
      internal_sink_->log(new_log_msg);
    }
    internal_sink_->flush();

    // If the last character for payload is already newliner, we've already flushed out
    // everything; otherwise need to keep left bytes in buffer.
    if (msg.payload[msg.payload.size() - 1] != '\n') {
      buffer_ = std::string{segments.back()};
    }
  }
  void flush_() override {
    if (!buffer_.empty()) {
      spdlog::details::log_msg new_log_msg;
      new_log_msg.payload = std::string_view{buffer_.data(), buffer_.length()};
      internal_sink_->log(std::move(new_log_msg));
      buffer_.clear();
    }
    internal_sink_->flush();
  }

 private:
  spdlog::sink_ptr internal_sink_;
  // Sink flushes in lines, buffer keeps unflushed bytes for the next new line.
  std::string buffer_;
};

using spdlog_newliner_sink_mt = spdlog_newliner_sink<std::mutex>;
using spdlog_newliner_sink_st = spdlog_newliner_sink<spdlog::details::null_mutex>;

}  // namespace ray

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

#include <sys/types.h>

#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "ray/common/status.h"
#include "ray/common/status_or.h"

/**
  RAII style class for creating and destroying temporary directory for testing.
  TODO(irabbani): add full documentation once complete.
  */
class TempDirectory {
 public:
  static ray::StatusOr<std::unique_ptr<TempDirectory>> Create();
  explicit TempDirectory(std::string &&path) : path_(path) {}

  TempDirectory(const TempDirectory &) = delete;
  TempDirectory(TempDirectory &&) = delete;
  TempDirectory &operator=(const TempDirectory &) = delete;
  TempDirectory &operator=(TempDirectory &&) = delete;

  const std::string &GetPath() const { return path_; }

  ~TempDirectory();

 private:
  const std::string path_;
};

/**
  RAII wrapper that creates a file that can be written to.
  TODO(irabbani): Add full documentation once the API is complete.
*/
class TempFile {
 public:
  explicit TempFile(std::string path);
  TempFile();

  TempFile(TempFile &other) = delete;
  TempFile(TempFile &&other) = delete;
  TempFile operator=(TempFile &other) = delete;
  TempFile &operator=(TempFile &&other) = delete;

  ~TempFile();
  void AppendLine(const std::string &line);

  const std::string &GetPath() const { return path_; }

 private:
  std::string path_ = "/tmp/XXXXXX";
  std::ofstream file_output_stream_;
  int fd_;
};

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
#include "ray/common/cgroup/memory_pressure_reader.h"

#include <climits>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

#include "gtest/gtest.h"
#include "ray/common/cgroup/mock_memory_pressure_reader.h"

namespace ray {

namespace {

class TempCgroupDir {
 public:
  TempCgroupDir() {
    char tmpl[] = "/tmp/ray_cgroup_test_XXXXXX";
    char *dir = ::mkdtemp(tmpl);
    path_ = dir == nullptr ? "" : dir;
  }
  ~TempCgroupDir() {
    if (!path_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(path_, ec);
    }
  }

  void Write(const std::string &name, const std::string &content) const {
    std::ofstream out(path_ + "/" + name);
    out << content;
  }

  const std::string &path() const { return path_; }

 private:
  std::string path_;
};

}  // namespace

TEST(MemoryPressureReaderTest, ReadsCgroupV2Values) {
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("memory.current", "123456789\n");
  dir.Write("memory.max", "987654321\n");

  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/true);
  int64_t current = 0;
  int64_t limit = 0;
  ASSERT_TRUE(reader.Read(&current, &limit).ok());
  EXPECT_EQ(current, 123456789);
  EXPECT_EQ(limit, 987654321);
  EXPECT_TRUE(reader.is_cgroup_v2());
}

TEST(MemoryPressureReaderTest, ReadsCgroupV1Values) {
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("memory.usage_in_bytes", "2048\n");
  dir.Write("memory.limit_in_bytes", "8192\n");

  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/false);
  int64_t current = 0;
  int64_t limit = 0;
  ASSERT_TRUE(reader.Read(&current, &limit).ok());
  EXPECT_EQ(current, 2048);
  EXPECT_EQ(limit, 8192);
  EXPECT_FALSE(reader.is_cgroup_v2());
}

TEST(MemoryPressureReaderTest, MaxSentinelMapsToInt64Max) {
  TempCgroupDir dir;
  dir.Write("memory.current", "10\n");
  dir.Write("memory.max", "max\n");
  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/true);
  int64_t current = 0;
  int64_t limit = 0;
  ASSERT_TRUE(reader.Read(&current, &limit).ok());
  EXPECT_EQ(limit, INT64_MAX);
}

TEST(MemoryPressureReaderTest, MissingFileReturnsIOError) {
  FileMemoryPressureReader reader("/nonexistent_cgroup_path", /*force_v2=*/true);
  int64_t current = 0;
  int64_t limit = 0;
  Status s = reader.Read(&current, &limit);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(MemoryPressureReaderTest, MalformedContentReturnsIOError) {
  TempCgroupDir dir;
  dir.Write("memory.current", "not_a_number\n");
  dir.Write("memory.max", "10\n");
  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/true);
  int64_t c = 0, l = 0;
  EXPECT_TRUE(reader.Read(&c, &l).IsIOError());
}

// Blind spot 6: missing robustness. In theory cgroup files always hold clean
// non-negative integers, but a sysfs read may hit a race (file just created,
// not yet written -> empty) or an abnormal kernel value. Read must degrade these
// to IOError rather than UB / crash. The three cases below cover empty / negative
// / overflow respectively.
TEST(MemoryPressureReaderTest, EmptyFileReturnsIOError) {
  // Arrange: memory.current exists but is empty (getline gets no line).
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("memory.current", "");
  dir.Write("memory.max", "1000\n");
  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/true);

  // Act
  int64_t c = 0, l = 0;
  Status s = reader.Read(&c, &l);

  // Assert: empty content takes the "empty content" branch and returns IOError.
  EXPECT_TRUE(s.IsIOError());
}

TEST(MemoryPressureReaderTest, NegativeValueReturnsIOError) {
  // Arrange: the kernel never writes negative values, but stoll can parse "-5",
  // which must be explicitly rejected by the v<0 check.
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("memory.current", "-5\n");
  dir.Write("memory.max", "1000\n");
  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/true);

  // Act
  int64_t c = 0, l = 0;
  Status s = reader.Read(&c, &l);

  // Assert: a negative value takes the "negative value" branch.
  EXPECT_TRUE(s.IsIOError());
}

TEST(MemoryPressureReaderTest, OverflowValueReturnsIOError) {
  // Blind spot 6: a numeric string exceeding the INT64 limit -> stoll throws
  // out_of_range -> caught and converted to IOError; it must not overflow into an
  // undefined value. Twenty 9s far exceed INT64_MAX (about 9.2e18).
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("memory.current", "99999999999999999999\n");
  dir.Write("memory.max", "1000\n");
  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/true);

  // Act
  int64_t c = 0, l = 0;
  Status s = reader.Read(&c, &l);

  // Assert
  EXPECT_TRUE(s.IsIOError());
}

// Blind spot 3: the real-vs-synthetic gap. Real cgroup file values carry a
// trailing '\n', and some kernels/mounts also append spaces or '\r'. A synthetic
// test that only feeds "123" (no trailing chars) would miss regressions in the
// trim loop. Here we feed a realistic "123456789  \t\r\n" and assert it still
// parses exactly after trimming.
TEST(MemoryPressureReaderTest, TrailingWhitespaceTrimmedBeforeParse) {
  // Arrange
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("memory.current", "123456789  \t\r\n");
  dir.Write("memory.max", "987654321\t \n");
  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/true);

  // Act
  int64_t current = 0, limit = 0;
  Status s = reader.Read(&current, &limit);

  // Assert: trailing whitespace does not contaminate the value.
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(current, 123456789);
  EXPECT_EQ(limit, 987654321);
}

// Blind spot 6: partial-failure ordering. When current is readable but the limit
// file is missing, Read must return IOError at the second ReadInt64File failure,
// rather than partially filling the out params and reporting success.
TEST(MemoryPressureReaderTest, LimitFileMissingReturnsIOError) {
  // Arrange: write only current, not limit.
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("memory.current", "100\n");
  FileMemoryPressureReader reader(dir.path(), /*force_v2=*/true);

  // Act
  int64_t c = 0, l = 0;
  Status s = reader.Read(&c, &l);

  // Assert: missing limit -> overall IOError.
  EXPECT_TRUE(s.IsIOError());
}

// The single-arg constructor is the only path on the C++ side that covers the
// "specify root + auto-probe" branch: the two-arg constructor's force_v2 is a
// literal that bypasses probing. The two cases below cover the v2 / v1 quadrants
// of probing respectively, guarding against the single-arg constructor inverting
// the probe semantics (e.g. always v2 or always v1).
TEST(MemoryPressureReaderTest, SingleArgCtorProbesCgroupV2WhenControllersFilePresent) {
  // Arrange: write the v2 probe file cgroup.controllers + v2 memory files.
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("cgroup.controllers", "memory io\n");
  dir.Write("memory.current", "555\n");
  dir.Write("memory.max", "1000\n");

  // Act: single-arg constructor, probes on its own.
  FileMemoryPressureReader reader(dir.path());
  int64_t current = 0, limit = 0;
  Status s = reader.Read(&current, &limit);

  // Assert: cgroup.controllers detected -> determined as v2, reads v2 files.
  EXPECT_TRUE(reader.is_cgroup_v2());
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(current, 555);
  EXPECT_EQ(limit, 1000);
}

TEST(MemoryPressureReaderTest, SingleArgCtorFallsBackToV1WhenControllersFileAbsent) {
  // Arrange: don't write cgroup.controllers (probe should determine v1), write
  // v1 memory files.
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  dir.Write("memory.usage_in_bytes", "333\n");
  dir.Write("memory.limit_in_bytes", "800\n");

  // Act
  FileMemoryPressureReader reader(dir.path());
  int64_t current = 0, limit = 0;
  Status s = reader.Read(&current, &limit);

  // Assert: probe file missing -> fall back to v1, reads v1 files.
  EXPECT_FALSE(reader.is_cgroup_v2());
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(current, 333);
  EXPECT_EQ(limit, 800);
}

TEST(MemoryPressureReaderTest, SingleArgCtorFallsBackToV1WhenProbeErrors) {
  // Arrange: construct a symlink loop so that probing cgroup.controllers
  // triggers ELOOP.
  TempCgroupDir dir;
  ASSERT_FALSE(dir.path().empty());
  const std::string loop_path = dir.path() + "/loop";
  std::error_code ec;
  std::filesystem::create_directory_symlink(loop_path, loop_path, ec);
  if (ec) {
    GTEST_SKIP() << "failed to create symlink loop: " << ec.message();
  }

  // Act / Assert: a probe error must not throw from the constructor; it should be
  // treated as missing and fall back to v1.
  EXPECT_NO_THROW({
    FileMemoryPressureReader reader(loop_path);
    EXPECT_FALSE(reader.is_cgroup_v2());
  });
}

TEST(MockMemoryPressureReaderTest, ReplaysScriptInOrder) {
  MockMemoryPressureReader mock;
  mock.Push(100, 1000);
  mock.Push(200, 1000);
  mock.Push(300, 1000);

  int64_t c = 0, l = 0;
  ASSERT_TRUE(mock.Read(&c, &l).ok());
  EXPECT_EQ(c, 100);
  ASSERT_TRUE(mock.Read(&c, &l).ok());
  EXPECT_EQ(c, 200);
  ASSERT_TRUE(mock.Read(&c, &l).ok());
  EXPECT_EQ(c, 300);
  // Further calls replay last entry.
  ASSERT_TRUE(mock.Read(&c, &l).ok());
  EXPECT_EQ(c, 300);
  EXPECT_EQ(mock.calls(), 4);
}

TEST(MockMemoryPressureReaderTest, PropagatesErrors) {
  MockMemoryPressureReader mock;
  mock.PushError("boom");
  int64_t c = 0, l = 0;
  EXPECT_TRUE(mock.Read(&c, &l).IsIOError());
}

}  // namespace ray

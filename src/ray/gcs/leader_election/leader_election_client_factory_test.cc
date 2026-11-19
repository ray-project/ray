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

#include "ray/gcs/leader_election/leader_election_client_factory.h"

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "ray/util/env.h"

namespace ray {
namespace gcs {

namespace {
constexpr char kServiceAccountNamespaceFile[] =
    "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
}  // namespace

class LeaderLeaseClientFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (const char *value = std::getenv("POD_NAMESPACE"); value != nullptr) {
      saved_pod_namespace_ = value;
    }
    UnsetEnv("POD_NAMESPACE");
  }

  void TearDown() override {
    if (saved_pod_namespace_.has_value()) {
      SetEnv("POD_NAMESPACE", *saved_pod_namespace_);
    } else {
      UnsetEnv("POD_NAMESPACE");
    }
  }

  // The fallthrough cases assert that no namespace is found, which a real pod's
  // service account file would contradict.
  static bool HasServiceAccountNamespaceFile() {
    return std::filesystem::exists(kServiceAccountNamespaceFile);
  }

 private:
  std::optional<std::string> saved_pod_namespace_;
};

TEST_F(LeaderLeaseClientFactoryTest, CreatesClientForConfiguredNamespace) {
  EXPECT_NE(LeaderLeaseClientFactory::Create("ray-system", "gcs-leader-lock"), nullptr);
}

TEST_F(LeaderLeaseClientFactoryTest, UsesConfiguredNamespace) {
  EXPECT_EQ(LeaderLeaseClientFactory::ResolveLeaseNamespace("ray-system"), "ray-system");
}

TEST_F(LeaderLeaseClientFactoryTest, ConfiguredNamespaceWinsOverPodNamespace) {
  SetEnv("POD_NAMESPACE", "pod-ns");
  EXPECT_EQ(LeaderLeaseClientFactory::ResolveLeaseNamespace("ray-system"), "ray-system");
}

TEST_F(LeaderLeaseClientFactoryTest, ConfiguredNamespaceIsTrimmed) {
  EXPECT_EQ(LeaderLeaseClientFactory::ResolveLeaseNamespace("  ray-system \n"),
            "ray-system");
}

TEST_F(LeaderLeaseClientFactoryTest, EmptyLeaseKeyCrashes) {
  EXPECT_DEATH(LeaderLeaseClientFactory::Create("ray-system", ""),
               "gcs_leader_election_lease_name must be set");
}

TEST_F(LeaderLeaseClientFactoryTest, FallsBackToPodNamespace) {
  SetEnv("POD_NAMESPACE", "pod-ns");
  EXPECT_EQ(LeaderLeaseClientFactory::ResolveLeaseNamespace(""), "pod-ns");
}

TEST_F(LeaderLeaseClientFactoryTest, PodNamespaceIsTrimmed) {
  SetEnv("POD_NAMESPACE", "  pod-ns\n");
  EXPECT_EQ(LeaderLeaseClientFactory::ResolveLeaseNamespace(""), "pod-ns");
}

TEST_F(LeaderLeaseClientFactoryTest, WhitespaceConfiguredNamespaceFallsThrough) {
  SetEnv("POD_NAMESPACE", "pod-ns");
  EXPECT_EQ(LeaderLeaseClientFactory::ResolveLeaseNamespace("   "), "pod-ns");
}

TEST_F(LeaderLeaseClientFactoryTest, NoNamespaceAnywhereCrashes) {
  if (HasServiceAccountNamespaceFile()) {
    GTEST_SKIP() << "Running in a pod with a service account namespace file.";
  }
  EXPECT_DEATH(LeaderLeaseClientFactory::Create("", "gcs-leader-lock"),
               "Could not determine the namespace");
}

TEST_F(LeaderLeaseClientFactoryTest, EmptyPodNamespaceFallsThrough) {
  if (HasServiceAccountNamespaceFile()) {
    GTEST_SKIP() << "Running in a pod with a service account namespace file.";
  }
  SetEnv("POD_NAMESPACE", "");
  EXPECT_EQ(LeaderLeaseClientFactory::ResolveLeaseNamespace(""), "");
  EXPECT_DEATH(LeaderLeaseClientFactory::Create("", "gcs-leader-lock"),
               "Could not determine the namespace");
}

TEST_F(LeaderLeaseClientFactoryTest, WhitespacePodNamespaceFallsThrough) {
  if (HasServiceAccountNamespaceFile()) {
    GTEST_SKIP() << "Running in a pod with a service account namespace file.";
  }
  SetEnv("POD_NAMESPACE", " \t\n");
  EXPECT_EQ(LeaderLeaseClientFactory::ResolveLeaseNamespace("   "), "");
  EXPECT_DEATH(LeaderLeaseClientFactory::Create("   ", "gcs-leader-lock"),
               "Could not determine the namespace");
}

}  // namespace gcs
}  // namespace ray

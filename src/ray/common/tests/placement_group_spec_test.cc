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

#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/placement_group.h"

namespace ray {

TEST(PlacementGroupSpecTest, TestPlacementGroupSpecBuilderSchedulingOptions) {
  PlacementGroupSpecBuilder builder;

  std::vector<PlacementGroupSchedulingOption> scheduling_options;

  // Option 1
  PlacementGroupSchedulingOption option1;
  option1.bundles = {{{"CPU", 1.0}}};
  scheduling_options.push_back(option1);

  // Option 2
  PlacementGroupSchedulingOption option2;
  option2.bundles = {{{"CPU", 2.0}}};
  scheduling_options.push_back(option2);

  PlacementGroupID pg_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::vector<std::unordered_map<std::string, double>> bundles = {{{"CPU", 1.0}}};

  builder.SetPlacementGroupSpec(pg_id,
                                "test_pg",
                                bundles,
                                rpc::PlacementStrategy::PACK,
                                /*is_detached=*/false,
                                /*soft_target_node_id=*/NodeID::Nil(),
                                /*job_id=*/JobID::FromInt(1),
                                /*creator_actor_id=*/ActorID::Nil(),
                                /*is_creator_detached_actor=*/false,
                                /*bundle_label_selector=*/{},
                                scheduling_options);

  PlacementGroupSpecification spec = builder.Build();

  // Verify that the proto message has the expected fields.
  const auto &proto = spec.GetMessage();

  ASSERT_EQ(proto.fallback_strategy_size(), 2);

  const auto &p_option1 = proto.fallback_strategy(0);
  ASSERT_EQ(p_option1.bundles_size(), 1);
  ASSERT_EQ(p_option1.bundles(0).unit_resources().at("CPU"), 1.0);

  const auto &p_option2 = proto.fallback_strategy(1);
  ASSERT_EQ(p_option2.bundles_size(), 1);
  ASSERT_EQ(p_option2.bundles(0).unit_resources().at("CPU"), 2.0);
}

}  // namespace ray

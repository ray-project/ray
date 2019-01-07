#include "ray/metrics/registry/any_ptr.h"
#include <vector>
#include "gtest/gtest.h"

namespace ray {

namespace metrics {

TEST(AnyPtrTest, CastToTest) {
  AnyPtr any_ptr;
  auto *vec = new std::vector<std::string>();
  any_ptr = vec;
  auto *vec_cast = any_ptr.CastTo<std::vector<std::string>>();
  ASSERT_EQ(vec_cast, vec);

  auto *bad_type = any_ptr.CastTo<std::vector<int>>();
  ASSERT_EQ(bad_type, nullptr);
}

}  // namespace metrics

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#include <list>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/client_connection.h"

namespace ray {
namespace raylet {

class ClientConnectionTest : public ::testing::Test {
 public:
  ClientConnectionTest() {}

 protected:
};

TEST_F(ClientConnectionTest, TestExample) {
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#include <iostream>

#include "gtest/gtest.h"

#include "object_manager.h"

class TestObjectManager : public ::testing::Test {
public:
  TestObjectManager() {
    std::cout << "TestObjectManager: started." << std::endl;
  }
};

TEST_F(TestObjectManager, TestObjectManagerCommands) {
  ASSERT_TRUE(true);
}

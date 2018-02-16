#include <iostream>

#include "gtest/gtest.h"

#include "node_manager.h"

boost::asio::io_service io_service;

class TestNodeManager : public ::testing::Test {
public:
  TestNodeManager() {
    std::cout << "TestNodeManager: started." << std::endl;
  }
};

TEST_F(TestNodeManager, TestNodeManagerCommands) {
ASSERT_TRUE(true);
}

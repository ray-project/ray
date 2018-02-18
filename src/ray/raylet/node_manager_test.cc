#include <iostream>

#include "gtest/gtest.h"

#include "node_manager.h"

boost::asio::io_service io_service;

class TestNodeManager : public ::testing::Test {
public:
  TestNodeManager() {
    std::cout << "TestNodeManager: started." << std::endl;

    boost::asio::io_service io_service;
    NodeServer server(io_service, std::string("hello"));
    io_service.run();
  }
};

TEST_F(TestNodeManager, TestNodeManagerCommands) {
  ASSERT_TRUE(true);
}

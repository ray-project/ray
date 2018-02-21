#include <iostream>

#include "gtest/gtest.h"

#include "node_manager.h"

boost::asio::io_service io_service;

class TestNodeManager : public ::testing::Test {
public:
  TestNodeManager() {
    std::cout << "TestNodeManager: started." << std::endl;
    std::unordered_map<string, double> static_resource_config;
    static_resource_config = {{"num_cpus", 1}, {"num_gpus", 1}};
    ray::ResourceSet resource_config(std::move(static_resource_config));

    boost::asio::io_service io_service;
    ray::NodeServer server(io_service, std::string("hello"), resource_config);
    io_service.run();
  }
};

TEST_F(TestNodeManager, TestNodeManagerCommands) {
  ASSERT_TRUE(true);
}

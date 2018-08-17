#include <cstdlib>
#include <iostream>

#include "gtest/gtest.h"
#include "ray/util/logging.h"

// This is not really test.
// This file just print some information using the logging macro.

TEST(PrintLogTest, BaseTest){
  RAY_LOG(DEBUG) << "This is the DEBUG message";
  RAY_LOG(INFO) << "This is the INFO message";
  RAY_LOG(WARNING) << "This is the WARNING message";
  RAY_LOG(ERROR) << "This is the ERROR message";
}

int main(int argc, char **argv) {
  ray::internal::RayLog::StartRayLog(argv[0], RAY_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  ray::internal::RayLog::ShutDownRayLog();
  return RUN_ALL_TESTS();
}
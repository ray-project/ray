// Copyright 2017 The Ray Authors.
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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/object_manager/plasma/client.h"

namespace plasma {

class MyImpl : public std::enable_shared_from_this<MyImpl> {
 public:
  MyImpl(  /// The connection to the store service.
      std::shared_ptr<StoreConn> store_conn);
  ~MyImpl(){};

  int64_t store_capacity_;
  /// The connection to the store service.
  std::shared_ptr<StoreConn> store_conn_;
};

MyImpl::MyImpl(  /// The connection to the store service.
    std::shared_ptr<StoreConn> store_conn)
    : store_capacity_(0), store_conn_(store_conn) {}

class MyClient {
 public:
  MyClient() : impl_(std::make_shared<MyImpl>(store_conn_)) {}

  void Fun() {}

 private:
  std::shared_ptr<StoreConn> store_conn_;
  std::shared_ptr<MyImpl> impl_;
};

class MyClass {
 public:
  ~MyClass() { std::cout << "~MyClass\n"; }
};

TEST(RemotePlasmaClientTest, ConstructorTest) {
  RemotePlasmaClient client;
  client.Connect("kkk");
  //   std::shared_ptr<StoreConn> store_conn = nullptr;
  //   std::make_shared<MyImpl>(store_conn);
  //   MyClient client;
  //   client.Fun();
}

TEST(RemotePlasmaClientTest, SharedPtrTest) {
  auto ptrA = std::make_shared<MyClass>();
  auto ptrB = ptrA;
  ptrB.reset();
  ptrA.reset();
}
}  // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
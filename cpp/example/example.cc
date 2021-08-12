// Copyright 2021 The Ray Authors.
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

/// This is an example of Ray C++ application. Please visit
/// `https://docs.ray.io/en/master/index.html` for more details.

/// including the `<ray/api.h>` header
#include <ray/api.h>

/// using namespace
using namespace ::ray::api;

/// common function
int Plus(int x, int y) { return x + y; }
/// Declare remote function
RAY_REMOTE(Plus);

/// class
class Counter {
 public:
  int count;

  Counter(int init) { count = init; }
  /// static factory method
  static Counter *FactoryCreate(int init) { return new Counter(init); }
  /// non static function
  int Add(int x) {
    count += x;
    return count;
  }
};

/// Declare remote function
RAY_REMOTE(Counter::FactoryCreate, &Counter::Add);

int main(int argc, char **argv) {
  /// configuration and initialization
  RayConfig config;
  Ray::Init(config);

  /// put and get object
  auto object = Ray::Put(100);
  auto put_get_result = *(Ray::Get(object));
  std::cout << "put_get_result = " << put_get_result << std::endl;

  /// common task
  auto task_object = Ray::Task(Plus).Remote(1, 2);
  int task_result = *(Ray::Get(task_object));
  std::cout << "task_result = " << task_result << std::endl;

  /// actor
  ActorHandle<Counter> actor = Ray::Actor(Counter::FactoryCreate).Remote(0);
  /// actor task
  auto actor_object = actor.Task(&Counter::Add).Remote(3);
  int actor_task_result = *(Ray::Get(actor_object));
  std::cout << "actor_task_result = " << actor_task_result << std::endl;
  /// actor task with reference argument
  auto actor_object2 = actor.Task(&Counter::Add).Remote(task_object);
  int actor_task_result2 = *(Ray::Get(actor_object2));
  std::cout << "actor_task_result2 = " << actor_task_result2 << std::endl;

  /// shutdown
  Ray::Shutdown();
  return 0;
}

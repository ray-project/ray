/// This is an example of Ray C++ application. Please visit
/// `https://docs.ray.io/en/master/index.html` for more details.

/// including the `<ray/api.h>` header
#include <ray/api.h>

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

class KVStore {
 public:
  KVStore(std::string dest_actor_name, int role)
      : dest_actor_name_(dest_actor_name), role_(role) {
    if (role_ == 1) {
      dest_actor_ = ray::Actor(KVStore::Create)
                        .SetMaxRestarts(1)
                        .SetName(dest_actor_name)
                        .Remote(dest_actor_name, 0);
    }

    was_restared_ = ray::WasCurrentActorRestarted();
    if (was_restared_) {
      dest_actor_ = ray::GetActor<KVStore>(dest_actor_name_);
      if (dest_actor_) {
        auto dest_actor = *dest_actor_;
        data_ = *dest_actor.Task(&KVStore::GetAllData).Remote().Get();
      } else {
        RAYLOG(INFO) << "no dest actor\n";
      }
    }
    RAYLOG(INFO) << "KVStore created\n";
  }

  static KVStore *Create(std::string dest_actor_name, int role) {
    return new KVStore(dest_actor_name, role);
  }

  std::unordered_map<std::string, std::string> GetAllData() {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_;
  }

  std::pair<bool, std::string> Get(std::string key) {
    std::unique_lock<std::mutex> lock(mtx_);
    auto it = data_.find(key);
    if (it == data_.end()) {
      return std::pair<bool, std::string>{};
    }

    return {true, it->second};
  }

  void Put(std::string key, std::string val) {
    if (role_ == 0) {
      return;
    }
    std::unique_lock<std::mutex> lock(mtx_);
    dest_actor_ = ray::GetActor<KVStore>(dest_actor_name_);
    if (dest_actor_) {
      RAYLOG(INFO) << "sync to dest actor";
      (*dest_actor_).Task(&KVStore::SyncData).Remote(key, val).Get();
    } else {
      RAYLOG(INFO) << "no dest actor";
    }

    RAYLOG(INFO) << "role: " << role_ << ", put data";

    data_[key] = val;
  }

  void SyncData(std::string key, std::string val) {
    std::unique_lock<std::mutex> lock(mtx_);
    data_[key] = val;
  }

  bool Del(std::string key) {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_.erase(key) > 0;
  }

  int GetRole() const { return role_; }

  bool WasRestarted() const { return was_restared_; }

 private:
  std::string dest_actor_name_;
  int role_ = 0;  // 0:slave, 1:master
  bool was_restared_ = false;
  std::unordered_map<std::string, std::string> data_;
  std::mutex mtx_;

  boost::optional<ray::ActorHandle<KVStore>> dest_actor_;
};

RAY_REMOTE(KVStore::Create, &KVStore::GetAllData, &KVStore::Get, &KVStore::Put,
           &KVStore::SyncData, &KVStore::Del, &KVStore::GetRole, &KVStore::WasRestarted);

int main(int argc, char **argv) {
  /// initialization
  ray::Init();

  {
    auto print_actor = [](auto &acotr) {
      bool was_restarted = *acotr.Task(&KVStore::WasRestarted).Remote().Get();
      int role = *acotr.Task(&KVStore::GetRole).Remote().Get();
      acotr.Task(&KVStore::Put).Remote("hello", "world").Get();
      auto pair = *acotr.Task(&KVStore::Get).Remote("hello").Get();

      std::cout << "was_restarted: " << was_restarted << ", "
                << "role: " << role << ", val: " << pair.second << "\n";
      auto all_data = *acotr.Task(&KVStore::GetAllData).Remote().Get();
      for (auto &pair : all_data) {
        std::cout << pair.first << ":" << pair.second << "\n";
      }
    };
    ray::ActorHandle<KVStore> master_actor = ray::Actor(KVStore::Create)
                                                 .SetMaxRestarts(1)
                                                 .SetName("master_actor")
                                                 .Remote("slave_actor", 1);
    print_actor(master_actor);

    auto dest_actor = ray::GetActor<KVStore>("slave_actor");
    print_actor(*dest_actor);
    // ray::ActorHandle<KVStore> slave_actor = ray::Actor(KVStore::Create)
    //                                             .SetMaxRestarts(1)
    //                                             .SetName("slave_actor")
    //                                             .Remote("master_actor", 0);
    // print_actor(slave_actor);
  }

  /// put and get object
  auto object = ray::Put(100);
  auto put_get_result = *(ray::Get(object));
  std::cout << "put_get_result = " << put_get_result << std::endl;

  /// common task
  auto task_object = ray::Task(Plus).Remote(1, 2);
  int task_result = *(ray::Get(task_object));
  std::cout << "task_result = " << task_result << std::endl;

  /// actor
  ray::ActorHandle<Counter> actor = ray::Actor(Counter::FactoryCreate).Remote(0);
  /// actor task
  auto actor_object = actor.Task(&Counter::Add).Remote(3);
  int actor_task_result = *(ray::Get(actor_object));
  std::cout << "actor_task_result = " << actor_task_result << std::endl;
  /// actor task with reference argument
  auto actor_object2 = actor.Task(&Counter::Add).Remote(task_object);
  int actor_task_result2 = *(ray::Get(actor_object2));
  std::cout << "actor_task_result2 = " << actor_task_result2 << std::endl;

  /// shutdown
  ray::Shutdown();
  return 0;
}

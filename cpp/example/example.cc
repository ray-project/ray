/// This is an example of Ray C++ application. Please visit
/// `https://docs.ray.io/en/master/index.html` for more details.

/// including the `<ray/api.h>` header
#include <ray/api.h>

enum Role {
  SLAVE = 0,
  MASTER = 1,
};
MSGPACK_ADD_ENUM(Role);

inline std::string RoleStr(Role role) { return role == Role::SLAVE ? "Slave" : "Master"; }

class KVStore {
 public:
  KVStore(const std::string &dest_actor_name, Role role)
      : dest_actor_name_(dest_actor_name), role_(role) {
    if (role_ == Role::MASTER) {
      // Create slave actor if the role is master.
      dest_actor_ = ray::Actor(KVStore::Create)
                        .SetMaxRestarts(1)
                        .SetName(dest_actor_name)
                        .Remote(dest_actor_name, Role::SLAVE);
    }

    was_restared_ = ray::WasCurrentActorRestarted();
    if (was_restared_) {
      if (role_ == Role::SLAVE) {
        dest_actor_ = ray::GetActor<KVStore>(dest_actor_name_);
        assert(dest_actor_);
      }

      // Pull all data from dest actor when restarted.
      data_ = *dest_actor_.get().Task(&KVStore::GetAllData).Remote().Get();
      RAYLOG(INFO) << RoleStr(role_) << " Pulled all data from dest actor";
    }
    RAYLOG(INFO) << RoleStr(role_) << " KVStore created";
  }

  static KVStore *Create(const std::string &dest_actor_name, Role role) {
    return new KVStore(dest_actor_name, role);
  }

  std::unordered_map<std::string, std::string> GetAllData() {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_;
  }

  std::pair<bool, std::string> Get(const std::string &key) {
    std::unique_lock<std::mutex> lock(mtx_);
    auto it = data_.find(key);
    if (it == data_.end()) {
      return std::pair<bool, std::string>{};
    }

    return {true, it->second};
  }

  void Put(const std::string &key, const std::string &val) {
    // Only master actor can put.
    assert(role_ == Role::MASTER);
    (*dest_actor_).Task(&KVStore::SyncData).Remote(key, val).Get();
    std::unique_lock<std::mutex> lock(mtx_);
    data_[key] = val;
  }

  void SyncData(const std::string &key, const std::string &val) {
    assert(role_ == Role::SLAVE);
    std::unique_lock<std::mutex> lock(mtx_);
    data_[key] = val;
  }

  bool Del(const std::string &key) {
    if (role_ == Role::SLAVE) {
      std::unique_lock<std::mutex> lock(mtx_);
      return data_.erase(key) > 0;
    }

    (*dest_actor_).Task(&KVStore::Del).Remote(key).Get();
    std::unique_lock<std::mutex> lock(mtx_);
    return data_.erase(key) > 0;
  }

  Role GetRole() const { return role_; }

  bool WasRestarted() const { return was_restared_; }

  bool Empty() {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_.empty();
  }

  size_t Size() {
    std::unique_lock<std::mutex> lock(mtx_);
    return data_.size();
  }

 private:
  std::string dest_actor_name_;
  Role role_ = Role::SLAVE;
  bool was_restared_ = false;
  std::unordered_map<std::string, std::string> data_;
  std::mutex mtx_;

  boost::optional<ray::ActorHandle<KVStore>> dest_actor_;
};

RAY_REMOTE(KVStore::Create, &KVStore::GetAllData, &KVStore::Get, &KVStore::Put,
           &KVStore::SyncData, &KVStore::Del, &KVStore::GetRole, &KVStore::WasRestarted,
           &KVStore::Empty, &KVStore::Size);

void PrintActor(ray::ActorHandle<KVStore> &actor) {
  Role role = *actor.Task(&KVStore::GetRole).Remote().Get();
  bool empty = *actor.Task(&KVStore::Empty).Remote().Get();

  if (empty) {
    std::cout << RoleStr(role) << " is empty.\n";
    return;
  }

  auto pair = *actor.Task(&KVStore::Get).Remote("hello").Get();
  size_t size = *actor.Task(&KVStore::Size).Remote().Get();
  bool was_restarted = *actor.Task(&KVStore::WasRestarted).Remote().Get();
  std::cout << RoleStr(role) << " was_restarted: " << was_restarted << ", size: " << size
            << ", val: " << pair.second << "\n";
  auto all_data = *actor.Task(&KVStore::GetAllData).Remote().Get();
  for (auto &pair : all_data) {
    std::cout << pair.first << " : " << pair.second << "\n";
  }
}

int main(int argc, char **argv) {
  /// initialization
  ray::Init();

  ray::ActorHandle<KVStore> master_actor = ray::Actor(KVStore::Create)
                                               .SetMaxRestarts(1)
                                               .SetName("master_actor")
                                               .Remote("slave_actor", Role::MASTER);
  master_actor.Task(&KVStore::Put).Remote("hello", "world").Get();
  PrintActor(master_actor);

  auto dest_actor = ray::GetActor<KVStore>("slave_actor");
  PrintActor(*dest_actor);

  master_actor.Task(&KVStore::Del).Remote("hello").Get();
  PrintActor(master_actor);
  PrintActor(*dest_actor);

  /// shutdown
  ray::Shutdown();
  return 0;
}

/// This is an example of Ray C++ application. Please visit
/// `https://docs.ray.io/en/master/index.html` for more details.

/// including the `<ray/api.h>` header
#include <ray/api.h>

#include <chrono>
#include <thread>

enum Role {
  SLAVE = 0,
  MASTER = 1,
};
MSGPACK_ADD_ENUM(Role);

inline std::string RoleName(Role role) {
  return role == Role::SLAVE ? "slave_actor" : "master_actor";
}

inline std::string DestRoleName(Role role) {
  return role == Role::SLAVE ? "master_actor" : "slave_actor";
}

class KVStore {
 public:
  KVStore(Role role) : role_(role) {
    was_restared_ = ray::WasCurrentActorRestarted();
    if (was_restared_) {
      HanldeFaileover();
    } else {
      if (role_ == Role::MASTER) {
        // Create slave actor if the role is master.
        dest_actor_ = ray::Actor(KVStore::Create)
                          .SetMaxRestarts(1)
                          .SetName(RoleName(Role::SLAVE))
                          .Remote(Role::SLAVE);
      }
    }
    RAYLOG(INFO) << RoleName(role_) << " KVStore created";
  }

  static KVStore *Create(Role role) { return new KVStore(role); }

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
  bool CheckAlive(Role role) {
    auto actor = ray::GetActor<KVStore>(RoleName(role));
    auto r = (*actor).Task(&KVStore::GetRole).Remote();
    std::vector<ray::ObjectRef<Role>> objects{r};
    auto result = ray::Wait(objects, objects.size(), 2000);
    if (result.ready.empty()) {
      return false;
    }

    dest_actor_ = actor;
    return true;
  }

  void HanldeFaileover() {
    bool r = CheckAlive(Role::SLAVE);
    if (r) {
      role_ = Role::MASTER;
    } else {
      r = CheckAlive(Role::MASTER);
      if (r) {
        role_ = Role::SLAVE;
      }
    }

    assert(r);
    auto actor = ray::GetActor<KVStore>(RoleName(Role::SLAVE));
    data_ = *actor.get().Task(&KVStore::GetAllData).Remote().Get();
    RAYLOG(INFO) << RoleName(role_) << " pull all data from " << DestRoleName(role_)
                 << " was_restared:" << was_restared_ << ", data size: " << data_.size();
  }

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
  bool was_restarted = *actor.Task(&KVStore::WasRestarted).Remote().Get();

  if (empty) {
    std::cout << RoleName(role) << " is empty, "
              << "was_restarted: " << was_restarted << "\n";
    return;
  }

  auto pair = *actor.Task(&KVStore::Get).Remote("hello").Get();
  size_t size = *actor.Task(&KVStore::Size).Remote().Get();

  std::cout << RoleName(role) << " was_restarted: " << was_restarted << ", size: " << size
            << ", val: " << pair.second << "\n";
}

void BasiceUsage() {
  ray::ActorHandle<KVStore> master_actor = ray::Actor(KVStore::Create)
                                               .SetMaxRestarts(5)
                                               .SetName(RoleName(Role::MASTER))
                                               .Remote(Role::MASTER);
  master_actor.Task(&KVStore::Put).Remote("hello", "world").Get();
  PrintActor(master_actor);

  auto slave_actor = *ray::GetActor<KVStore>(RoleName(Role::SLAVE));
  PrintActor(slave_actor);

  master_actor.Task(&KVStore::Del).Remote("hello").Get();
  PrintActor(master_actor);
  PrintActor(slave_actor);
  master_actor.Task(&KVStore::Put).Remote("hello", "world").Get();
  PrintActor(master_actor);
  PrintActor(slave_actor);
}

void Faileover() {
  auto master_actor = *ray::GetActor<KVStore>(RoleName(Role::MASTER));
  master_actor.Kill(false);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  PrintActor(master_actor);

  auto slave_actor = *ray::GetActor<KVStore>(RoleName(Role::SLAVE));
  PrintActor(slave_actor);
}

ray::PlacementGroup CreateSimplePlacementGroup(const std::string &name) {
  std::vector<std::unordered_map<std::string, double>> bundles{{{"CPU", 1}, {"CPU", 1}}};

  ray::internal::PlacementGroupCreationOptions options{
      false, name, bundles, ray::internal::PlacementStrategy::SPREAD};
  return ray::CreatePlacementGroup(options);
}

void PlacementGroup() {
  auto placement_group = CreateSimplePlacementGroup("first_placement_group");
  assert(ray::WaitPlacementGroupReady(placement_group.GetID(), 10));

  ray::ActorHandle<KVStore> master_actor = ray::Actor(KVStore::Create)
                                               .SetMaxRestarts(5)
                                               .SetPlacementGroup(placement_group, 0)
                                               .SetName(RoleName(Role::MASTER))
                                               .Remote(Role::MASTER);
  master_actor.Task(&KVStore::Put).Remote("hello", "world").Get();
  PrintActor(master_actor);

  auto slave_actor = *ray::GetActor<KVStore>(RoleName(Role::SLAVE));
  PrintActor(slave_actor);

  master_actor.Kill(false);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  PrintActor(master_actor);

  ray::RemovePlacementGroup(placement_group.GetID());
}

int main(int argc, char **argv) {
  /// initialization
  ray::Init();
  std::shared_ptr<int> guard(nullptr, [](int *p) { ray::Shutdown(); });
  // PlacementGroup();
  BasiceUsage();
  Faileover();

  /// shutdown
  // ray::Shutdown();
  return 0;
}

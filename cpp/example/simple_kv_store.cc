/// This is an example of Ray C++ application. Please visit
/// `https://docs.ray.io/en/master/index.html` for more details.

/// including the `<ray/api.h>` header
#include <ray/api.h>

#include <chrono>
#include <thread>

const std::string MAIN_SERVER_NAME = "main_actor";
const std::string BACKUP_SERVER_NAME = "backup_actor";
const std::unordered_map<std::string, double> RESOUECES{{"CPU", 1.0},
                                                        {"memory", 1024.0 * 1024.0}};

namespace common {
inline std::pair<bool, std::string> Get(
    const std::string &key, const std::unordered_map<std::string, std::string> &data) {
  auto it = data.find(key);
  if (it == data.end()) {
    return std::pair<bool, std::string>{};
  }

  return {true, it->second};
}
}  // namespace common

class MainServer;
class BackupServer {
 public:
  BackupServer();

  // The main server will get all BackupServer's data when it restarted.
  std::unordered_map<std::string, std::string> GetAllData();

  // The main server will sync data at first before puting data.
  void SyncData(const std::string &key, const std::string &val);

 private:
  // Get all data from MainServer.
  void HanldeFailover();

  std::unordered_map<std::string, std::string> data_;
  std::mutex mtx_;
};

BackupServer::BackupServer() {
  // Handle failover when the actor restarted.
  if (ray::WasCurrentActorRestarted()) {
    HanldeFailover();
  }
  RAYLOG(INFO) << "BackupServer created";
}

std::unordered_map<std::string, std::string> BackupServer::GetAllData() {
  std::unique_lock<std::mutex> lock(mtx_);
  return data_;
}

void BackupServer::SyncData(const std::string &key, const std::string &val) {
  std::unique_lock<std::mutex> lock(mtx_);
  data_[key] = val;
}

class MainServer {
 public:
  MainServer();

  void Put(const std::string &key, const std::string &val);

  std::pair<bool, std::string> Get(const std::string &key);

  std::unordered_map<std::string, std::string> GetAllData();

 private:
  void HanldeFailover();

  std::unordered_map<std::string, std::string> data_;
  std::mutex mtx_;

  ray::ActorHandle<BackupServer> backup_actor_;
};

MainServer::MainServer() {
  if (ray::WasCurrentActorRestarted()) {
    HanldeFailover();
  }

  backup_actor_ = *ray::GetActor<BackupServer>(BACKUP_SERVER_NAME);
  RAYLOG(INFO) << "MainServer created";
}

void MainServer::Put(const std::string &key, const std::string &val) {
  // SyncData before put data.
  auto r = backup_actor_.Task(&BackupServer::SyncData).Remote(key, val);
  std::vector<ray::ObjectRef<void>> objects{r};
  auto result = ray::Wait(objects, 1, 2000);
  if (result.ready.empty()) {
    RAYLOG(WARNING) << "MainServer SyncData failed.";
  }

  std::unique_lock<std::mutex> lock(mtx_);
  data_[key] = val;
}

std::pair<bool, std::string> MainServer::Get(const std::string &key) {
  std::unique_lock<std::mutex> lock(mtx_);
  return common::Get(key, data_);
}

std::unordered_map<std::string, std::string> MainServer::GetAllData() {
  std::unique_lock<std::mutex> lock(mtx_);
  return data_;
}

void BackupServer::HanldeFailover() {
  // Get all data from MainServer.
  auto dest_actor = *ray::GetActor<MainServer>(MAIN_SERVER_NAME);
  data_ = *dest_actor.Task(&MainServer::GetAllData).Remote().Get();
  RAYLOG(INFO) << "BackupServer get all data from MainServer";
}

void MainServer::HanldeFailover() {
  // Get all data from BackupServer.
  backup_actor_ = *ray::GetActor<BackupServer>(BACKUP_SERVER_NAME);
  data_ = *backup_actor_.Task(&BackupServer::GetAllData).Remote().Get();
  RAYLOG(INFO) << "MainServer get all data from BackupServer";
}

static MainServer *CreateMainServer() { return new MainServer(); }

static BackupServer *CreateBackupServer() { return new BackupServer(); }

RAY_REMOTE(CreateMainServer, &MainServer::GetAllData, &MainServer::Get, &MainServer::Put);

RAY_REMOTE(CreateBackupServer, &BackupServer::GetAllData, &BackupServer::SyncData);

ray::PlacementGroup CreateSimplePlacementGroup(const std::string &name) {
  std::vector<std::unordered_map<std::string, double>> bundles{RESOUECES, RESOUECES};

  ray::PlacementGroupCreationOptions options{false, name, bundles,
                                             ray::PlacementStrategy::SPREAD};
  return ray::CreatePlacementGroup(options);
}

void StartServer() {
  auto placement_group = CreateSimplePlacementGroup("my_placement_group");
  assert(placement_group.Wait(10));

  ray::Actor(CreateMainServer)
      .SetName("main_actor")
      .SetResources(RESOUECES)
      .SetPlacementGroup(placement_group, 0)
      .SetMaxRestarts(1)
      .Remote();

  ray::Actor(CreateBackupServer)
      .SetName("backup_actor")
      .SetResources(RESOUECES)
      .SetPlacementGroup(placement_group, 1)
      .SetMaxRestarts(1)
      .Remote();
}

void KillMainServer() {
  auto main_server = *ray::GetActor<MainServer>(MAIN_SERVER_NAME);
  main_server.Kill(false);
  std::this_thread::sleep_for(std::chrono::seconds(2));
}

class Client {
 public:
  Client() : main_actor_(*ray::GetActor<MainServer>(MAIN_SERVER_NAME)) {}

  void Put(const std::string &key, const std::string &val) {
    main_actor_.Task(&MainServer::Put).Remote(key, val).Get();
  }

  std::pair<bool, std::string> Get(const std::string &key) {
    return *main_actor_.Task(&MainServer::Get).Remote(key).Get();
  }

 private:
  ray::ActorHandle<MainServer> main_actor_;
};

int main(int argc, char **argv) {
  /// Start ray cluster and ray runtime.
  ray::Init();

  StartServer();

  Client client;
  client.Put("hello", "ray");

  auto get_result = [&client](const std::string &key) {
    bool ok;
    std::string result;
    std::tie(ok, result) = client.Get("hello");
    assert(ok);
    assert(result == "ray");
  };

  get_result("hello");

  // Kill main server and then get result.
  KillMainServer();
  get_result("hello");

  /// Stop ray cluster and ray runtime.
  ray::Shutdown();
  return 0;
}

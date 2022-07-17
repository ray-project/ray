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
#include <ray/api.h>

#include <chrono>
#include <thread>

// Name of the main server actor.
const std::string MAIN_SERVER_NAME = "main_actor";
// Name of the backup server actor.
const std::string BACKUP_SERVER_NAME = "backup_actor";
// Resources that each actor needs: 1 CPU core and 1 GB memory.
const std::unordered_map<std::string, double> RESOUECES{
    {"CPU", 1.0}, {"memory", 1024.0 * 1024.0 * 1024.0}};

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

  // The main server will get all BackupServer's data when it restarts.
  std::unordered_map<std::string, std::string> GetAllData();

  // This is used for the main server to backup data before putting data.
  void BackupData(const std::string &key, const std::string &val);

 private:
  // When the backup server restarts from a failure. It will get all data from the main
  // server.
  void HandleFailover();
  // A map that stores the key-value data.
  std::unordered_map<std::string, std::string> data_;
};

BackupServer::BackupServer() {
  // Handle failover when the actor restarts.
  if (ray::WasCurrentActorRestarted()) {
    HandleFailover();
  }
  RAYLOG(INFO) << "BackupServer created";
}

std::unordered_map<std::string, std::string> BackupServer::GetAllData() { return data_; }

void BackupServer::BackupData(const std::string &key, const std::string &val) {
  data_[key] = val;
}

class MainServer {
 public:
  MainServer();

  // Set value by the given key.
  void Put(const std::string &key, const std::string &val);

  // Get value from a key.
  std::pair<bool, std::string> Get(const std::string &key);

  // This is used for the backup server to recover data when it restarts.
  std::unordered_map<std::string, std::string> GetAllData();

 private:
  // When the main server restarts from a failure. It will get all data from the backup
  // server.
  void HandleFailover();
  // A map that stores the key-value data.
  std::unordered_map<std::string, std::string> data_;
  // Actor handle to the backup server actor.
  ray::ActorHandle<BackupServer> backup_actor_;
};

MainServer::MainServer() {
  if (ray::WasCurrentActorRestarted()) {
    HandleFailover();
  }

  backup_actor_ = *ray::GetActor<BackupServer>(BACKUP_SERVER_NAME);
  RAYLOG(INFO) << "MainServer created";
}

void MainServer::Put(const std::string &key, const std::string &val) {
  // Backup data before putting data.
  auto r = backup_actor_.Task(&BackupServer::BackupData).Remote(key, val);
  std::vector<ray::ObjectRef<void>> objects{r};
  auto result = ray::Wait(objects, objects.size(), 2000);
  if (result.ready.empty()) {
    RAYLOG(WARNING) << "MainServer BackupData failed.";
  }

  data_[key] = val;
}

std::pair<bool, std::string> MainServer::Get(const std::string &key) {
  return common::Get(key, data_);
}

std::unordered_map<std::string, std::string> MainServer::GetAllData() { return data_; }

void BackupServer::HandleFailover() {
  // Get all data from MainServer.
  auto dest_actor = *ray::GetActor<MainServer>(MAIN_SERVER_NAME);
  data_ = *dest_actor.Task(&MainServer::GetAllData).Remote().Get();
  RAYLOG(INFO) << "BackupServer recovered all data from MainServer";
}

void MainServer::HandleFailover() {
  // Get all data from BackupServer.
  backup_actor_ = *ray::GetActor<BackupServer>(BACKUP_SERVER_NAME);
  data_ = *backup_actor_.Task(&BackupServer::GetAllData).Remote().Get();
  RAYLOG(INFO) << "MainServer recovered all data from BackupServer";
}

static MainServer *CreateMainServer() { return new MainServer(); }

static BackupServer *CreateBackupServer() { return new BackupServer(); }

RAY_REMOTE(CreateMainServer, &MainServer::GetAllData, &MainServer::Get, &MainServer::Put);

RAY_REMOTE(CreateBackupServer, &BackupServer::GetAllData, &BackupServer::BackupData);

void StartServer() {
  // Start the main server and the backup server.
  // Each of them needs 1 cpu core and 1 GB memory.
  // We use Ray's placement group to make sure that the 2 actors will be scheduled to 2
  // different nodes if possible.
  std::vector<std::unordered_map<std::string, double>> bundles{RESOUECES, RESOUECES};

  ray::PlacementGroupCreationOptions options{
      "kv_server_pg", bundles, ray::PlacementStrategy::SPREAD};
  auto placement_group = ray::CreatePlacementGroup(options);
  // Wait until the placement group is created.
  assert(placement_group.Wait(10));

  // Create the main server actor and backup server actor.
  ray::Actor(CreateMainServer)
      .SetName(MAIN_SERVER_NAME)              // Set name of this actor.
      .SetResources(RESOUECES)                // Set the resources that this actor needs.
      .SetPlacementGroup(placement_group, 0)  // Set the corresponding placement group.
      .SetMaxRestarts(-1)  // Tell Ray to restart this actor automatically upon failures.
      .Remote();

  ray::Actor(CreateBackupServer)
      .SetName(BACKUP_SERVER_NAME)
      .SetResources(RESOUECES)
      .SetPlacementGroup(placement_group, 1)
      .SetMaxRestarts(-1)
      .Remote();
}

void KillMainServer() {
  auto main_server = *ray::GetActor<MainServer>(MAIN_SERVER_NAME);
  main_server.Kill(false);
}

class Client {
 public:
  Client() {
    main_actor_ = ray::GetActor<MainServer>(MAIN_SERVER_NAME);
    // In case the main server hasn't started yet, retry getting
    // the actor handle.
    if (!main_actor_) {
      RetryGetActor();
    }
  }

  bool Put(const std::string &key, const std::string &val) {
    // During the restart of the main server actor, the task may fail.
    // So we use retry here.
    AlwaysRetry([this, &key, &val] {
      (*main_actor_).Task(&MainServer::Put).Remote(key, val).Get();
      return true;
    });

    return true;
  }

  std::pair<bool, std::string> Get(const std::string &key) {
    // During the restart of the main server actor, the task may fail.
    // So we use retry here.
    return AlwaysRetry([this, &key] {
      return *(*main_actor_).Task(&MainServer::Get).Remote(key).Get();
    });
  }

 private:
  void RetryGetActor() {
    AlwaysRetry([this] {
      main_actor_ = ray::GetActor<MainServer>(MAIN_SERVER_NAME);
      return std::make_pair(bool(main_actor_), "");
    });
  }

  template <typename F>
  std::result_of_t<F()> AlwaysRetry(const F &f) {
    using R = std::result_of_t<F()>;
    R r{};
    while (true) {
      try {
        r = f();
        if (IsValid(r)) {
          break;
        }
      } catch (const std::exception &) {
      }

      std::cout << "Retry in 2 seconds\n";
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }

    return r;
  }

  bool IsValid(bool r) { return r; }

  bool IsValid(const std::pair<bool, std::string> r) { return r.first; }

  boost::optional<ray::ActorHandle<MainServer>> main_actor_;
};

int main(int argc, char **argv) {
  // Start ray cluster and ray runtime.
  ray::RayConfig config;
  ray::Init(config, argc, argv);

  StartServer();

  Client client;
  bool r = client.Put("hello", "ray");
  assert(r);
  // Silence warning.
  (void)r;

  auto get_result = [&client](const std::string &key) {
    bool ok;
    std::string result;
    std::tie(ok, result) = client.Get("hello");
    assert(ok);
    assert(result == "ray");
  };

  get_result("hello");

  // Kill main server. It should restart automatically and recover data from the backup
  // server.
  KillMainServer();
  get_result("hello");

  // Stop ray cluster and ray runtime.
  ray::Shutdown();
  return 0;
}

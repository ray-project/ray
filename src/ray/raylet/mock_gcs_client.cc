#include <iostream>
#include <functional>

#include "ray/raylet/mock_gcs_client.h"

using namespace std;

namespace ray {

ray::Status ObjectTable::GetObjectClientIDs(const ray::ObjectID &object_id,
                                            const ClientIDsCallback &success,
                                            const FailCallback &fail){
  // cout << "GetObjectClientIDs " << object_id.hex() << endl;
  if (client_lookup.count(object_id) > 0) {
    if (!client_lookup[object_id].empty()) {
      vector<ClientID> v;
      for (auto client_id : client_lookup[object_id]) {
        v.push_back(client_id);
      }
      success(std::move(v));
      return Status::OK();
    } else {
      fail(Status::KeyError("ObjectID has no clients."));
      return Status::OK();
    }
  } else {
    fail(Status::KeyError("ObjectID doesn't exist."));
    return Status::OK();
  }
};

ray::Status ObjectTable::Add(const ObjectID &object_id,
                             const ClientID &client_id,
                             const DoneCallback &done){
  if (client_lookup.count(object_id) == 0){
    // cout << "Add ObjectID set " << object_id.hex() << endl;
    client_lookup[object_id] = std::unordered_set<ClientID, UniqueIDHasher>();
  } else if (client_lookup[object_id].count(client_id) != 0){
    return ray::Status::KeyError("ClientID already exists.");
  }
  // cout << "Insert ClientID " << client_id.hex() << endl;
  client_lookup[object_id].insert(client_id);
  done();
  return ray::Status::OK();
};

ray::Status ObjectTable::Remove(const ObjectID &object_id,
                                const ClientID &client_id,
                                const DoneCallback &done){
  if (client_lookup.count(object_id) == 0){
    return ray::Status::KeyError("ObjectID doesn't exist.");
  } else if (client_lookup[object_id].count(client_id) == 0){
    return ray::Status::KeyError("ClientID doesn't exist.");
  }
  client_lookup[object_id].erase(client_id);
  done();
  return ray::Status::OK();
};


ray::Status ClientTable::GetClientIds(ClientIDsCallback cb){
  std::vector<ClientID> keys;
  keys.reserve(info_lookup.size());
  for(auto kv : info_lookup) {
    keys.push_back(kv.first);
  }
  cb(keys);
  return Status::OK();
};

void ClientTable::GetClientInformationSet(const vector<ClientID> &client_ids,
                                          ManyInfoCallback cb,
                                          FailCallback failcb){
  std::vector<ClientInformation> info_vec;
  for(const auto &client_id : client_ids){
    if (info_lookup.count(client_id) != 0){
      info_vec.push_back(info_lookup.at(client_id));
    }
  }
  if(info_vec.empty()){
    failcb(Status::KeyError("ClientID not found."));
  } else {
    cb(info_vec);
  }
}

void ClientTable::GetClientInformation(ClientID client_id,
                                       SingleInfoCallback cb,
                                       FailCallback failcb){
  if (info_lookup.count(client_id) == 0){
    failcb(ray::Status::KeyError("CleintID not found."));
  } else {
    cb(info_lookup.at(client_id));
  }
}

ray::Status ClientTable::Add(const ClientID &client_id,
                             const std::string &ip,
                             ushort port,
                             DoneCallback done){
  if (info_lookup.count(client_id) != 0){
    return ray::Status::KeyError("ClientID already exists.");
  }
  info_lookup.emplace(client_id, ClientInformation(client_id, ip, port));
  done();
  return ray::Status::OK();
};

ray::Status ClientTable::Remove(const ClientID &client_id,
                                DoneCallback done){
  if (info_lookup.count(client_id) == 0){
    return ray::Status::KeyError("ClientID doesn't exist.");
  }
  info_lookup.erase(client_id);
  done();
  return ray::Status::OK();
};


ClientID GcsClient::Register(const std::string &ip, ushort port) {
  ClientID client_id = ClientID().from_random();
  // TODO: handle client registration failure.
  ray::Status status =
      client_table().Add(std::move(client_id), ip, port, [](){});
  return client_id;
};

ObjectTable &GcsClient::object_table(){
  return *object_table_;
};

ClientTable &GcsClient::client_table(){
  return *client_table_;
};

};

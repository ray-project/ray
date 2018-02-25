#include <ray/raylet/mock_gcs_client.h>

namespace ray {

const std::unordered_set<ClientID, UniqueIDHasher> &ObjectTable::GetObjectClientIDs(const ray::ObjectID &object_id){
  if (client_lookup.count(object_id) > 0){
    return client_lookup[object_id];
  } else {
    return empty_set_;
  };
};

ray::Status ObjectTable::Add(const ObjectID &object_id, const ClientID &client_id){
  if (client_lookup.count(object_id) == 0){
    client_lookup[object_id] = std::unordered_set<ClientID, UniqueIDHasher>();
  } else if (client_lookup[object_id].count(client_id) != 0){
    return ray::Status::KeyError("ClientID already exists.");
  }
  client_lookup[object_id].insert(client_id);
  return ray::Status::OK();
};

ray::Status ObjectTable::Remove(const ObjectID &object_id, const ClientID &client_id){
  if (client_lookup.count(object_id) == 0){
    return ray::Status::KeyError("ObjectID doesn't exist.");
  } else if (client_lookup[object_id].count(client_id) == 0){
    return ray::Status::KeyError("ClientID doesn't exist.");
  }
  client_lookup[object_id].erase(client_id);
  return ray::Status::OK();
};


const ClientInformation &ClientTable::GetClientInformation(const ClientID &client_id){
  if (info_lookup.count(client_id) == 0){
    throw std::runtime_error("ClientID doesn't exist.");
  }
  return info_lookup[client_id];
}

ray::Status ClientTable::Add(const ClientID &client_id,
                             const std::string ip,
                             int port){
  if (info_lookup.count(client_id) != 0){
    return ray::Status::KeyError("ClientID already exists.");
  }
  info_lookup[client_id] = std::move(ClientInformation(client_id, ip, port));
  return ray::Status::OK();
};

ray::Status ClientTable::Remove(const ClientID &client_id){
  if (info_lookup.count(client_id) == 0){
    return ray::Status::KeyError("ClientID doesn't exist.");
  }
  info_lookup.erase(client_id);
  return ray::Status::OK();
};


void GcsClient::Register(const std::string &ip, int port) {
  client_table()->Add(ClientID().from_random(), ip, port);
};

std::unique_ptr<ObjectTable> &GcsClient::object_table(){
  return object_table_;
};

std::unique_ptr<ClientTable> &GcsClient::client_table(){
  return client_table_;
};

};

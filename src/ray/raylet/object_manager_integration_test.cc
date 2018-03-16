#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#include "ray/object_manager/object_manager.h"

namespace ray {

namespace raylet {

class MockRaylet {

 private:

  boost::asio::ip::tcp::acceptor tcp_acceptor_;
  boost::asio::ip::tcp::socket tcp_socket_;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_;
  ObjectManager object_manager_;

 public:

  MockRaylet(boost::asio::io_service &io_service,
         const ObjectManagerConfig &object_manager_config,
         std::shared_ptr<gcs::AsyncGcsClient> gcs_client)
  : tcp_acceptor_(io_service,
                  boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0)),
    tcp_socket_(io_service),
    gcs_client_(gcs_client),
    object_manager_(io_service, object_manager_config, gcs_client)
  {
    ClientID client_id = RegisterGcs(io_service);
    object_manager_.SetClientID(client_id);
    // Start listening for clients.
    DoAcceptTcp();
  }

  ~MockRaylet() {
    (void) gcs_client_->client_table().Disconnect();
    RAY_CHECK_OK(object_manager_.Terminate());
  }

  ObjectManager &GetObjectManager() { return object_manager_; }

 private:

  ClientID RegisterGcs(boost::asio::io_service &io_service){
      boost::asio::ip::tcp::endpoint endpoint = tcp_acceptor_.local_endpoint();
      std::string ip = endpoint.address().to_string();
      unsigned short object_manager_port = endpoint.port();

      ClientID client_id = ClientID::from_random();

      ClientTableDataT client_info;
      client_info.client_id = client_id.binary();
      client_info.node_manager_address = ip;
      client_info.local_scheduler_port = object_manager_port;
      client_info.object_manager_port = object_manager_port;
      RAY_CHECK_OK(gcs_client_->Connect("127.0.0.1", 6379, client_info));
      RAY_CHECK_OK(gcs_client_->Attach(io_service));
      RAY_CHECK_OK(gcs_client_->client_table().Connect());
      return client_id;
  }

  void DoAcceptTcp() {
    TCPClientConnection::pointer new_connection =
        TCPClientConnection::Create(tcp_acceptor_.get_io_service());
    tcp_acceptor_.async_accept(new_connection->GetSocket(),
                               boost::bind(&MockRaylet::HandleAcceptTcp, this, new_connection,
                                           boost::asio::placeholders::error));
  }

  void HandleAcceptTcp(TCPClientConnection::pointer new_connection,
                               const boost::system::error_code &error) {
    if (!error) {
      // Pass it off to object manager for now.
      ray::Status status = object_manager_.AcceptConnection(std::move(new_connection));
    }
    DoAcceptTcp();
  }

};

std::string test_executable;  // NOLINT

class TestRaylet : public ::testing::Test {
 public:
  TestRaylet() { RAY_LOG(INFO) << "TestRaylet: started."; }

  std::string StartStore(const std::string &id) {
    std::string store_id = "/tmp/store";
    store_id = store_id + id;
    std::string test_dir = test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_dir = test_dir + "./../plasma";
    std::string plasma_command = plasma_dir + "/plasma_store -m 1000000000 -s " +
                                 store_id + " 1> /dev/null 2> /dev/null &";
    RAY_LOG(INFO) << plasma_command;
    int ec = system(plasma_command.c_str());
    if (ec != 0) {
      throw std::runtime_error("failed to start plasma store.");
    };
    return store_id;
  }

  void SetUp() {
    // start store
    std::string store_sock_1 = StartStore("1");
    std::string store_sock_2 = StartStore("2");

    // start first server
    gcs_client_1 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = store_sock_1;
    server1.reset(new MockRaylet(io_service, om_config_1, gcs_client_1));

    // start second server
    gcs_client_2 = std::shared_ptr<gcs::AsyncGcsClient>(new gcs::AsyncGcsClient());
    ObjectManagerConfig om_config_2;
    om_config_2.store_socket_name = store_sock_2;
    server2.reset(new MockRaylet(io_service, om_config_2, gcs_client_2));

    // connect to stores.
    ARROW_CHECK_OK(client1.Connect(store_sock_1, "", PLASMA_DEFAULT_RELEASE_DELAY));
    ARROW_CHECK_OK(client2.Connect(store_sock_2, "", PLASMA_DEFAULT_RELEASE_DELAY));
  }

  void TearDown() {
    arrow::Status client1_status = client1.Disconnect();
    arrow::Status client2_status = client2.Disconnect();
    ASSERT_TRUE(client1_status.ok() && client2_status.ok());

    this->server1.reset();
    this->server2.reset();

    int s = system("killall plasma_store &");
    ASSERT_TRUE(!s);

//    std::string cmd_str = test_executable.substr(0, test_executable.find_last_of("/"));
//    s = system(("rm " + cmd_str + "/hello1").c_str());
//    ASSERT_TRUE(!s);
//    s = system(("rm " + cmd_str + "/hello2").c_str());
//    ASSERT_TRUE(!s);
  }

  ObjectID WriteDataToClient(plasma::PlasmaClient &client, int64_t data_size) {
    ObjectID object_id = ObjectID::from_random();
    RAY_LOG(DEBUG) << "ObjectID Created: " << object_id.hex().c_str();
    uint8_t metadata[] = {5};
    int64_t metadata_size = sizeof(metadata);
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client.Create(object_id.to_plasma_id(), data_size, metadata,
                                 metadata_size, &data));
    ARROW_CHECK_OK(client.Seal(object_id.to_plasma_id()));
    return object_id;
  }

  void object_added_handler_1(ObjectID object_id) {
    v1.push_back(object_id);
    // RAY_LOG(INFO) << "Store 1 size: " << v1.size();
  };

  void object_added_handler_2(ObjectID object_id) {
    v2.push_back(object_id);
    // RAY_LOG(INFO) << "Store 2 size: " << v2.size();
  };

 protected:
  std::thread p;
  boost::asio::io_service io_service;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_1;
  std::shared_ptr<gcs::AsyncGcsClient> gcs_client_2;
  std::unique_ptr<MockRaylet> server1;
  std::unique_ptr<MockRaylet> server2;

  plasma::PlasmaClient client1;
  plasma::PlasmaClient client2;
  std::vector<ObjectID> v1;
  std::vector<ObjectID> v2;
};


class TestGCSIntegration : public TestRaylet {

 public:

  enum TransferPattern {
    PUSH_A_B,
    PUSH_B_A,
    BIDIRECTIONAL_PUSH,
    PULL_A_B,
    PULL_B_A,
    BIDIRECTIONAL_PULL,
  };

  int async_loop_index = -1;
  uint num_expected_objects;

  std::vector<TransferPattern> async_loop_patterns = {
       PUSH_A_B
      ,PUSH_B_A
      ,BIDIRECTIONAL_PUSH
      ,PULL_A_B
      ,PULL_B_A
      ,BIDIRECTIONAL_PULL
  };

  int num_connected_clients = 0;

  void WaitConnections(){
    gcs_client_1->client_table().RegisterClientAddedCallback(
        [this](gcs::AsyncGcsClient *client, const ClientID &id, std::shared_ptr<ClientTableDataT> data){
          num_connected_clients += 1;
          if(num_connected_clients == 2){
            StartTests();
          }
        }
    );
  }

  void StartTests(){
    TestConnections();
    AddTransferTestHandlers();
    TransferTestNext();
  }

  void AddTransferTestHandlers(){
    ray::Status status = ray::Status::OK();
    status = server1->GetObjectManager().SubscribeObjAdded(
        [this](const ObjectID &object_id) {
          object_added_handler_1(object_id);
          if(v1.size() == num_expected_objects && v1.size() == v2.size()){
            TransferTestComplete();
          }
        }
    );
    RAY_CHECK_OK(status);
    status = server2->GetObjectManager().SubscribeObjAdded(
        [this](const ObjectID &object_id) {
          object_added_handler_2(object_id);
          if(v2.size() == num_expected_objects && v1.size() == v2.size()){
            TransferTestComplete();
          }
        }
    );
    RAY_CHECK_OK(status);
  }

  void TransferTestNext(){
    async_loop_index += 1;
    if((uint) async_loop_index < async_loop_patterns.size()){
      TransferPattern pattern = async_loop_patterns[async_loop_index];
      TransferTestExecute(1000, 100, pattern);
    } else {
      io_service.stop();
    }
  }

  void TransferTestComplete(){
    RAY_LOG(INFO) << "TransferTestComplete: "
                  << async_loop_patterns[async_loop_index] << " "
                  << v1.size() << " "
                  << v2.size();
    ASSERT_TRUE(v1.size() == v2.size());
    for (int i = -1; ++i < (int) v1.size();) {
      ASSERT_TRUE(std::find(v1.begin(), v1.end(), v2[i]) != v1.end());
    }
    v1.clear();
    v2.clear();
    TransferTestNext();
  }

  void TransferTestExecute(int num_trials,
                           int64_t data_size,
                           TransferPattern transfer_pattern){

    ClientID client_id_1 = server1->GetObjectManager().GetClientID();
    ClientID client_id_2 = server2->GetObjectManager().GetClientID();

    ray::Status status = ray::Status::OK();

    if (transfer_pattern == BIDIRECTIONAL_PULL || transfer_pattern == BIDIRECTIONAL_PUSH) {
      num_expected_objects = (uint) 2*num_trials;
    } else {
      num_expected_objects = (uint) num_trials;
    }

    switch(transfer_pattern) {
      case PUSH_A_B: {
        for(int i=-1;++i<num_trials;) {
          ObjectID oid1 = WriteDataToClient(client1, data_size);
          status = server1->GetObjectManager().Push(oid1, client_id_2);
        }
      } break;
      case PUSH_B_A: {
        for(int i=-1;++i<num_trials;){
          ObjectID oid2 = WriteDataToClient(client2, data_size);
          status = server2->GetObjectManager().Push(oid2, client_id_1);
        }
      } break;
      case BIDIRECTIONAL_PUSH: {
        for(int i=-1;++i<num_trials;){
          ObjectID oid1 = WriteDataToClient(client1, data_size);
          status = server1->GetObjectManager().Push(oid1, client_id_2);
          ObjectID oid2 = WriteDataToClient(client2, data_size);
          status = server2->GetObjectManager().Push(oid2, client_id_1);
        }
      } break;
      case PULL_A_B: {
        for(int i=-1;++i<num_trials;){
          ObjectID oid1 = WriteDataToClient(client1, data_size);
          status = server2->GetObjectManager().Pull(oid1);
        }
      } break;
      case PULL_B_A: {
        for(int i=-1;++i<num_trials;){
          ObjectID oid2 = WriteDataToClient(client2, data_size);
          status = server1->GetObjectManager().Pull(oid2);
        }
      } break;
      case BIDIRECTIONAL_PULL: {
        for(int i=-1;++i<num_trials;){
          ObjectID oid1 = WriteDataToClient(client1, data_size);
          status = server2->GetObjectManager().Pull(oid1);
          ObjectID oid2 = WriteDataToClient(client2, data_size);
          status = server1->GetObjectManager().Pull(oid2);
        }
      } break;
    }

  }

  void TestConnections(){
    RAY_LOG(INFO) << "\n"
                  << "Server client ids:"
                  << "\n";
    ClientID client_id_1 = server1->GetObjectManager().GetClientID();
    ClientID client_id_2 = server2->GetObjectManager().GetClientID();
    RAY_LOG(INFO) << "Server 1: " << client_id_1.hex();
    RAY_LOG(INFO) << "Server 2: " << client_id_2.hex();

    RAY_LOG(INFO) << "\n"
                  << "All connected clients:"
                  << "\n";
    const ClientTableDataT &data = gcs_client_1->client_table().GetClient(client_id_1);
    RAY_LOG(INFO) << (ClientID::from_binary(data.client_id) == ClientID::nil());
    RAY_LOG(INFO) << "ClientID=" << ClientID::from_binary(data.client_id);
    RAY_LOG(INFO) << "ClientIp=" << data.node_manager_address;
    RAY_LOG(INFO) << "ClientPort=" << data.local_scheduler_port;
    const ClientTableDataT &data2 = gcs_client_1->client_table().GetClient(client_id_2);
    RAY_LOG(INFO) << "ClientID=" << ClientID::from_binary(data2.client_id);
    RAY_LOG(INFO) << "ClientIp=" << data2.node_manager_address;
    RAY_LOG(INFO) << "ClientPort=" << data2.local_scheduler_port;
  }

};

TEST_F(TestGCSIntegration, TestRayletCommands) {
  auto AsyncStartTests = io_service.wrap([this](){
    WaitConnections();
  });
  AsyncStartTests();
  io_service.run();
}

} // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ray::raylet::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}

namespace ray {
namespace gcs {

class MockInMemoryStoreClient : public InMemoryStoreClient {
 public:
  MOCK_METHOD(Status, AsyncPut, (const std::string &table_name, const std::string &key, const std::string &data, const StatusCallback &callback), (override));
  MOCK_METHOD(Status, AsyncPutWithIndex, (const std::string &table_name, const std::string &key, const std::string &index_key, const std::string &data, const StatusCallback &callback), (override));
  MOCK_METHOD(Status, AsyncGet, (const std::string &table_name, const std::string &key, const OptionalItemCallback<std::string> &callback), (override));
  MOCK_METHOD(Status, AsyncGetByIndex, (const std::string &table_name, const std::string &index_key, (const MapCallback<std::string, std::string> &callback)), (override));
  MOCK_METHOD(Status, AsyncGetAll, (const std::string &table_name, (const MapCallback<std::string, std::string> &callback)), (override));
  MOCK_METHOD(Status, AsyncDelete, (const std::string &table_name, const std::string &key, const StatusCallback &callback), (override));
  MOCK_METHOD(Status, AsyncDeleteWithIndex, (const std::string &table_name, const std::string &key, const std::string &index_key, const StatusCallback &callback), (override));
  MOCK_METHOD(Status, AsyncBatchDelete, (const std::string &table_name, const std::vector<std::string> &keys, const StatusCallback &callback), (override));
  MOCK_METHOD(Status, AsyncBatchDeleteWithIndex, (const std::string &table_name, const std::vector<std::string> &keys, const std::vector<std::string> &index_keys, const StatusCallback &callback), (override));
  MOCK_METHOD(Status, AsyncDeleteByIndex, (const std::string &table_name, const std::string &index_key, const StatusCallback &callback), (override));
  MOCK_METHOD(int, GetNextJobID, (), (override));
};

}  // namespace gcs
}  // namespace ray

namespace ray {

class MockClusterResourceSchedulerInterface : public ClusterResourceSchedulerInterface {
 public:
  MOCK_METHOD(bool, RemoveNode, (const std::string &node_id_string), (override));
  MOCK_METHOD(bool, UpdateNode, (const std::string &node_id_string, const rpc::ResourcesData &resource_data), (override));
  MOCK_METHOD(void, UpdateResourceCapacity, (const std::string &node_id_string, const std::string &resource_name, double resource_total), (override));
  MOCK_METHOD(void, DeleteResource, (const std::string &node_id_string, const std::string &resource_name), (override));
  MOCK_METHOD(void, UpdateLastResourceUsage, (const std::shared_ptr<SchedulingResources> gcs_resources), (override));
  MOCK_METHOD(void, FillResourceUsage, (rpc::ResourcesData &data), (override));
  MOCK_METHOD(double, GetLocalAvailableCpus, (), (const, override));
  MOCK_METHOD(ray::gcs::NodeResourceInfoAccessor::ResourceMap, GetResourceTotals, (), (const, override));
  MOCK_METHOD(std::string, GetLocalResourceViewString, (), (const, override));
};

}  // namespace ray

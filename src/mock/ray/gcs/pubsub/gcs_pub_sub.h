namespace ray {
namespace gcs {

class MockGcsPubSub : public GcsPubSub {
 public:
  MOCK_METHOD(Status, Publish, (const std::string &channel, const std::string &id, const std::string &data, const StatusCallback &done), (override));
};

}  // namespace gcs
}  // namespace ray

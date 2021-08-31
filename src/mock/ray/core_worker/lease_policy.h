namespace ray {
namespace core {

class MockLocalityData : public LocalityData {
 public:
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockLocalityDataProviderInterface : public LocalityDataProviderInterface {
 public:
  MOCK_METHOD(absl::optional<LocalityData>, GetLocalityData, (const ObjectID &object_id), (override));
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockLeasePolicyInterface : public LeasePolicyInterface {
 public:
  MOCK_METHOD(rpc::Address, GetBestNodeForTask, (const TaskSpecification &spec), (override));
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockLocalityAwareLeasePolicy : public LocalityAwareLeasePolicy {
 public:
};

}  // namespace core
}  // namespace ray

namespace ray {
namespace core {

class MockLocalLeasePolicy : public LocalLeasePolicy {
 public:
};

}  // namespace core
}  // namespace ray

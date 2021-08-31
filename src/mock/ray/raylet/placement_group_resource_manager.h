namespace ray {
namespace raylet {

class Mockpair_hash : public pair_hash {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockBundleTransactionState : public BundleTransactionState {
 public:
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockPlacementGroupResourceManager : public PlacementGroupResourceManager {
 public:
  MOCK_METHOD(bool, PrepareBundle, (const BundleSpecification &bundle_spec), (override));
  MOCK_METHOD(void, CommitBundle, (const BundleSpecification &bundle_spec), (override));
  MOCK_METHOD(void, ReturnBundle, (const BundleSpecification &bundle_spec), (override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockNewPlacementGroupResourceManager : public NewPlacementGroupResourceManager {
 public:
};

}  // namespace raylet
}  // namespace ray

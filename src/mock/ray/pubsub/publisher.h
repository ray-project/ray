namespace ray {
namespace pubsub {
namespace pub_internal {

template <typename KeyIdType>
class MockSubscriptionIndex : public SubscriptionIndex<KeyIdType> {
 public:
};

}  // namespace pub_internal
}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {
namespace pub_internal {

class MockLongPollConnection : public LongPollConnection {
 public:
};

}  // namespace pub_internal
}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {
namespace pub_internal {

class MockSubscriber : public Subscriber {
 public:
};

}  // namespace pub_internal
}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockPublisherInterface : public PublisherInterface {
 public:
  MOCK_METHOD(bool, RegisterSubscription, (const rpc::ChannelType channel_type, const SubscriberID &subscriber_id, const std::string &key_id_binary), (override));
  MOCK_METHOD(void, Publish, (const rpc::ChannelType channel_type, const rpc::PubMessage &pub_message, const std::string &key_id_binary), (override));
  MOCK_METHOD(void, PublishFailure, (const rpc::ChannelType channel_type, const std::string &key_id_binary), (override));
  MOCK_METHOD(bool, UnregisterSubscription, (const rpc::ChannelType channel_type, const SubscriberID &subscriber_id, const std::string &key_id_binary), (override));
};

}  // namespace pubsub
}  // namespace ray

namespace ray {
namespace pubsub {

class MockPublisher : public Publisher {
 public:
  MOCK_METHOD(bool, RegisterSubscription, (const rpc::ChannelType channel_type, const SubscriberID &subscriber_id, const std::string &key_id_binary), (override));
  MOCK_METHOD(void, Publish, (const rpc::ChannelType channel_type, const rpc::PubMessage &pub_message, const std::string &key_id_binary), (override));
  MOCK_METHOD(void, PublishFailure, (const rpc::ChannelType channel_type, const std::string &key_id_binary), (override));
  MOCK_METHOD(bool, UnregisterSubscription, (const rpc::ChannelType channel_type, const SubscriberID &subscriber_id, const std::string &key_id_binary), (override));
};

}  // namespace pubsub
}  // namespace ray

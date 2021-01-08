#include <thread>

#include "event_service.h"
#include "gtest/gtest.h"

using namespace ray::streaming;

/// Mock function for send empty message.
bool SendEmptyToChannel(ProducerChannelInfo *info) { return true; }

/// Mock function for write all messages to channel.
bool WriteAllToChannel(ProducerChannelInfo *info) { return true; }

TEST(EventServiceTest, Test1) {
  std::shared_ptr<EventService> server = std::make_shared<EventService>();

  ProducerChannelInfo mock_channel_info;
  server->Register(EventType::EmptyEvent, SendEmptyToChannel);
  server->Register(EventType::UserEvent, WriteAllToChannel);
  server->Register(EventType::FlowEvent, WriteAllToChannel);

  bool stop = false;
  std::thread thread_empty([server, &mock_channel_info, &stop] {
    std::chrono::milliseconds MockTimer(20);
    while (!stop) {
      Event event(&mock_channel_info, EventType::EmptyEvent, true);
      server->Push(event);
      std::this_thread::sleep_for(MockTimer);
    }
  });

  std::thread thread_flow([server, &mock_channel_info, &stop] {
    std::chrono::milliseconds MockTimer(2);
    while (!stop) {
      Event event(&mock_channel_info, EventType::FlowEvent, true);
      server->Push(event);
      std::this_thread::sleep_for(MockTimer);
    }
  });

  std::thread thread_user([server, &mock_channel_info, &stop] {
    std::chrono::milliseconds MockTimer(2);
    while (!stop) {
      Event event(&mock_channel_info, EventType::UserEvent, true);
      server->Push(event);
      std::this_thread::sleep_for(MockTimer);
    }
  });

  server->Run();

  std::this_thread::sleep_for(std::chrono::milliseconds(5 * 1000));

  STREAMING_LOG(INFO) << "5 seconds passed.";
  STREAMING_LOG(INFO) << "EventNums: " << server->EventNums();
  stop = true;
  STREAMING_LOG(INFO) << "Stop";
  server->Stop();
  thread_empty.join();
  thread_flow.join();
  thread_user.join();
}

TEST(EventServiceTest, remove_delete_channel_event) {
  std::shared_ptr<EventService> server = std::make_shared<EventService>();

  std::vector<ObjectID> channel_vec;
  std::vector<ProducerChannelInfo> mock_channel_info_vec;
  channel_vec.push_back(ObjectID::FromRandom());
  ProducerChannelInfo mock_channel_info1;
  mock_channel_info1.channel_id = channel_vec.back();
  mock_channel_info_vec.push_back(mock_channel_info1);
  ProducerChannelInfo mock_channel_info2;
  channel_vec.push_back(ObjectID::FromRandom());
  mock_channel_info2.channel_id = channel_vec.back();
  mock_channel_info_vec.push_back(mock_channel_info2);

  for (auto &id : mock_channel_info_vec) {
    Event empty_event(&id, EventType::EmptyEvent, true);
    Event user_event(&id, EventType::UserEvent, true);
    Event flow_event(&id, EventType::FlowEvent, true);
    server->Push(empty_event);
    server->Push(user_event);
    server->Push(flow_event);
  }
  std::vector<ObjectID> removed_vec(channel_vec.begin(), channel_vec.begin() + 1);
  server->RemoveDestroyedChannelEvent(removed_vec);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

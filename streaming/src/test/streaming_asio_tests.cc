#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "streaming.h"
#include "streaming_asio.h"
#include "test/test_utils.h"

using namespace ray::streaming;
using namespace ray;

class TestTask {
 private:
  uint32_t t_ = 0;
  std::mutex mutex_;

 public:
  void operator()() {
    std::lock_guard<std::mutex> lock_guard(mutex_);
    t_++;
  }

  void Callback() { STREAMING_LOG(INFO) << "callback, value =>" << t_; }
  uint32_t GetValue() { return t_; }
};

TEST(StreamingAsioTest, streaming_asio_test) {
  StreamingThreadPool *pool = new StreamingThreadPool(8, 4);
  pool->Start();
  TestTask test_task;
  for (uint32_t i = 0; i < 10; ++i) {
    pool->Post(
        std::make_shared<StreamingTask>(std::bind(&TestTask::operator(), &test_task),
                                        std::bind(&TestTask::Callback, &test_task)));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  delete pool;
  EXPECT_EQ(test_task.GetValue(), 10);
}

static int callback = 0;
static int lambda_callback = 0;
void run_task_callback() {
  callback++;
  STREAMING_LOG(INFO) << "run task callback invoked, callback value => " << callback;
}

void run_task(const StreamingMessageBundlePtr &message_bundle,
              const std::string &output_file) {
  std::shared_ptr<StreamingAsyncIO> asyn_IO_pool(new StreamingAsyncIO(8, 4));

  std::shared_ptr<StreamingIORunner> io_runner(
      new StreamingIORunner(message_bundle, output_file));
  std::shared_ptr<StreamingIORunner> io_runner2(
      new StreamingIORunner(message_bundle, output_file));
  // submit a io runner task to task pool
  asyn_IO_pool->Post(io_runner, std::move(run_task_callback));
  asyn_IO_pool->Post(io_runner2, []() {
    lambda_callback = 1;
    STREAMING_LOG(INFO) << "lambda callback, value =>" << lambda_callback;
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  STREAMING_CHECK(lambda_callback == 1);
  lambda_callback = 0;
}

#ifdef USE_PANGU
void run_task_pangu(StreamingMessageBundlePtr message_bundle,
                    const std::string &output_file) {
  std::shared_ptr<StreamingAsyncIO> asyn_IO_pool(new StreamingAsyncIO(8, 4));
  std::shared_ptr<StreamingFileIO> pangu_fs(new StreamingPanguFileSystem(output_file));
  std::shared_ptr<StreamingIORunner> io_runner(
      new StreamingIORunner(message_bundle, pangu_fs));
  // submit a io runner task to task pool
  asyn_IO_pool->Post(io_runner, std::move(run_task_callback));
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST(StreamingAsioTest, streaming_asio_pangu_writer_test) {
  std::string output_file("/zdfs_test/bundle_file");
  std::list<StreamingMessagePtr> message_list;
  for (int i = 0; i < 5; ++i) {
    uint8_t *data = new uint8_t[i + 1];
    std::memset(data, i, i + 1);
    StreamingMessagePtr message =
        util::MakeMessagePtr(data, i + 1, i + 1, StreamingMessageType::Message);

    message_list.push_back(message);
    delete[] data;
  }
  StreamingMessageBundlePtr message_bundle(new StreamingMessageBundle(
      message_list, 0, 100, StreamingMessageBundleType::Bundle));
  StreamingPanguFileSystem::Init();
  run_task_pangu(message_bundle, output_file);
  EXPECT_EQ(callback, 1);
  callback = 0;

  StreamingPanguFileSystem local_reader(output_file, true);
  std::shared_ptr<uint8_t> ptr(new uint8_t[message_bundle->ClassBytesSize()],
                               std::default_delete<uint8_t[]>());
  std::shared_ptr<uint8_t> de_ptr(new uint8_t[message_bundle->ClassBytesSize()],
                                  std::default_delete<uint8_t[]>());

  local_reader.Open();
  local_reader.Read(ptr.get(), message_bundle->ClassBytesSize());
  local_reader.Close();

  message_bundle->ToBytes(de_ptr.get());
  // remove dump file
  local_reader.Delete(output_file);
  StreamingPanguFileSystem::Destory();
  STREAMING_LOG(DEBUG) << StreamingUtility::Byte2hex(ptr.get(),
                                                     message_bundle->ClassBytesSize());
  STREAMING_LOG(DEBUG) << StreamingUtility::Byte2hex(de_ptr.get(),
                                                     message_bundle->ClassBytesSize());
  EXPECT_EQ(std::memcmp(de_ptr.get(), ptr.get(), message_bundle->ClassBytesSize()), 0);
}

TEST(StreamingAsioTest, streaming_asio_pangu_read_all_test) {
  std::string output_file("/zdfs_test/pangu_test_file");
  STREAMING_LOG(INFO) << "pangu read all test";
  StreamingPanguFileSystem::Init();
  StreamingPanguFileSystem local_file(output_file, true);
  local_file.Delete();

  size_t bytes_size = 100;
  std::shared_ptr<uint8_t> write_bytes(new uint8_t[bytes_size]);
  std::memset(write_bytes.get(), bytes_size, 1);

  StreamingPanguFileSystem local_writer(output_file);
  local_writer.Open();
  local_writer.Write(write_bytes.get(), bytes_size);
  local_writer.Close();

  StreamingPanguFileSystem local_reader(output_file, true);
  std::shared_ptr<uint8_t> ptr;
  local_reader.Open();
  uint32_t data_size = 0;
  local_reader.ReadAll(&ptr, data_size);
  local_reader.Close();

  local_reader.Delete(output_file);
  StreamingPanguFileSystem::Destory();
  EXPECT_EQ(std::memcmp(write_bytes.get(), ptr.get(), bytes_size), 0);
  EXPECT_EQ(data_size, bytes_size);
}
#endif

TEST(StreamingAsioTest, streaming_asio_writer_test) {
  std::string output_file("/tmp/bundle_file");
  std::list<StreamingMessagePtr> message_list;
  for (int i = 0; i < 5; ++i) {
    uint8_t *data = new uint8_t[i + 1];
    std::memset(data, i, i + 1);
    StreamingMessagePtr message =
        util::MakeMessagePtr(data, i + 1, i + 1, StreamingMessageType::Message);
    message_list.push_back(message);
    delete[] data;
  }
  StreamingMessageBundlePtr message_bundle(new StreamingMessageBundle(
      message_list, 0, 100, StreamingMessageBundleType::Bundle));

  run_task(message_bundle, output_file);
  EXPECT_EQ(callback, 1);
  callback = 0;

  StreamingLocalFileSystem local_reader(output_file, true);
  std::shared_ptr<uint8_t> ptr(new uint8_t[message_bundle->ClassBytesSize()],
                               std::default_delete<uint8_t[]>());
  std::shared_ptr<uint8_t> de_ptr(new uint8_t[message_bundle->ClassBytesSize()],
                                  std::default_delete<uint8_t[]>());

  local_reader.Open();
  local_reader.Read(ptr.get(), message_bundle->ClassBytesSize());
  local_reader.Close();

  message_bundle->ToBytes(de_ptr.get());
  // remove dump file
  STREAMING_CHECK(local_reader.Delete());

  STREAMING_LOG(DEBUG) << StreamingUtility::Byte2hex(ptr.get(),
                                                     message_bundle->ClassBytesSize());
  STREAMING_LOG(DEBUG) << StreamingUtility::Byte2hex(de_ptr.get(),
                                                     message_bundle->ClassBytesSize());
  EXPECT_EQ(std::memcmp(de_ptr.get(), ptr.get(), message_bundle->ClassBytesSize()), 0);
}

TEST(StreamingAsioTest, streaming_asio_read_all_test) {
  std::string output_file("/tmp/bundle_file");

  size_t bytes_size = 100;
  std::shared_ptr<uint8_t> write_bytes(new uint8_t[bytes_size]);
  std::memset(write_bytes.get(), bytes_size, 1);

  StreamingLocalFileSystem local_writer(output_file);
  local_writer.Open();
  local_writer.Write(write_bytes.get(), bytes_size);
  local_writer.Close();

  StreamingLocalFileSystem local_reader(output_file, true);
  std::shared_ptr<uint8_t> ptr;
  local_reader.Open();
  uint32_t data_size = 0;
  local_reader.ReadAll(&ptr, data_size);
  local_reader.Close();

  // remove dump file
  STREAMING_CHECK(local_reader.Delete());

  EXPECT_EQ(std::memcmp(write_bytes.get(), ptr.get(), bytes_size), 0);
  EXPECT_EQ(bytes_size, data_size);
}

int main(int argc, char **argv) {
  set_streaming_log_config("streaming_asio_tests", StreamingLogLevel::INFO);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

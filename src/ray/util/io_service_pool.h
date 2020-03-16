#ifndef RAY_UTIL_IO_SERVICE_POOL_H
#define RAY_UTIL_IO_SERVICE_POOL_H

#include <atomic>
#include <boost/asio.hpp>
#include <thread>

namespace ray {

/// \class IOServicePool
/// The io_service pool. Each io_service owns a thread.
/// To get io_service from this pool should call `Run()` first.
/// Before exit, `Stop()` must be called.
class IOServicePool {
 public:
  IOServicePool(size_t io_service_num);

  void Run();

  void Stop();

  /// Select io_service by round robin.
  ///
  /// \return io_service
  boost::asio::io_service &Get();

  /// Select io_service by hash.
  ///
  /// \param hash Use this hash to pick a io_service.
  /// The same hash will alway get the same io_service.
  /// \return io_service
  boost::asio::io_service &Get(size_t hash);

  /// Get all io_service.
  /// This is only use for RedisClient::Connect().
  std::vector<boost::asio::io_service &> GetAll();

 private:
  size_t io_service_num_{0};

  std::vector<std::thread> threads_;
  std::vector<boost::asio::io_service> io_services_;

  std::atomic<size_t> current_index_;
};

}  // namespace ray

#endif  // RAY_UTIL_IO_SERVICE_POOL_H

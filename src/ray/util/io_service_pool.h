#ifndef RAY_UTIL_IO_SERVICE_POOL_H
#define RAY_UTIL_IO_SERVICE_POOL_H

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

 private:
  std::vector<std::thread> threads_;
  std::vector<boost::io_service> io_services_;

  size_t current_index_;
};

}  // namespace ray

#endif  // RAY_UTIL_IO_SERVICE_POOL_H
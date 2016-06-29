#ifndef RAY_UTILS_H
#define RAY_UTILS_H

#include <mutex>
#include <string>

template<class Synchronized>
struct SynchronizedTarget {
  typedef typename Synchronized::element_type type;
};

template<class Synchronized>
struct SynchronizedTarget<const Synchronized> {
  typedef const typename Synchronized::element_type type;
};

template<class Synchronized>
class SynchronizedPtr : public std::unique_lock<Synchronized> {
  typedef std::unique_lock<Synchronized> base_type;

  // Make these private; they don't make much sense externally...
  using typename base_type::mutex_type;
  using base_type::mutex;
public:
  typedef typename SynchronizedTarget<Synchronized>::type value_type;
  SynchronizedPtr(Synchronized& value) : base_type(value) { }
  value_type& operator*() const { return *mutex()->unsafe_get(); }
  value_type* operator->() const { return mutex() ? mutex()->unsafe_get() : NULL; }
};

template<class T, class Mutex = std::mutex>
class Synchronized {
  mutable Mutex mutex_;
  T value_;
public:
  typedef T element_type;
  template<class... U>
  Synchronized(U&&... args) : value_(std::forward<T>(args)...) { }
  Synchronized(const Synchronized& other) : value_(*other) { }
  Synchronized(Synchronized&& other) : value_(std::move(*other)) { }
  Synchronized& operator =(const Synchronized& other) { *get() = *other.get(); return *this; }
  Synchronized& operator =(Synchronized&& other) { *get() = std::move(*other.get()); return *this; }
  void lock() const { return mutex_.lock(); }
  void unlock() const { return mutex_.unlock(); }
  bool try_lock() const { return mutex_.try_lock(); }
  SynchronizedPtr<Synchronized> get() { return *this; }
  SynchronizedPtr<const Synchronized> get() const { return *this; }
  element_type* unsafe_get() { return &value_; }
  const element_type* unsafe_get() const { return &value_; }
};

std::string::iterator split_ip_address(std::string& ip_address);

const char* get_cmd_option(char** begin, char** end, const std::string& option);

void create_log_dir_or_die(const char* log_file_name);

#endif

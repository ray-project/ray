#ifndef RAY_UTILS_H
#define RAY_UTILS_H

#include <mutex>
#include <string>

template<class T, class Mutex = std::mutex>
class Synchronized;

template<class T, class Mutex>
class Synchronized<const T, Mutex>;  // Prevent use of const T; it doesn't make sense

template<class T, class Mutex> struct SynchronizedSource { typedef Synchronized<T, Mutex> type; };
template<class T, class Mutex> struct SynchronizedSource<const T, Mutex> { typedef const Synchronized<T, Mutex> type; };
template<class T, class Mutex> struct SynchronizedSource<volatile T, Mutex> { typedef volatile Synchronized<T, Mutex> type; };
template<class T, class Mutex> struct SynchronizedSource<const volatile T, Mutex> { typedef const Synchronized<T, Mutex> type; };

template<class T>
class SynchronizedPtr : public std::unique_lock<typename SynchronizedSource<T, void>::type> {
  typedef std::unique_lock<typename SynchronizedSource<T, void>::type> base_type;
  // Make these private; they don't make much sense externally...
  using base_type::mutex;
public:
  typedef T value_type;
  SynchronizedPtr(typename base_type::mutex_type& value) : base_type(value) { }
  value_type& operator*() const { return *mutex()->unsafe_get(); }
  value_type* operator->() const { return mutex() ? mutex()->unsafe_get() : NULL; }
};

template<class T>
class Synchronized<T, void> {
  T value_;
public:
  typedef T element_type;
  template<class... U>
  Synchronized(U&&... args) : value_(std::forward<U>(args)...) { }
  Synchronized(const Synchronized& other) : value_((std::lock_guard<Synchronized>(other), other.value_)) { }
  Synchronized(Synchronized&& other) : value_((std::lock_guard<Synchronized>(other), std::move(other.value_))) { }
  Synchronized& operator =(const Synchronized& other)
  {
    if (this != &other)
    {
      std::lock_guard<Synchronized> guard_this(*this);
      std::lock_guard<Synchronized> guard_other(other);
      value_ = other.value_;
    }
    return *this;
  }
  Synchronized& operator =(Synchronized&& other)
  {
    if (this != &other)
    {
      std::lock_guard<Synchronized> guard_this(*this);
      std::lock_guard<Synchronized> guard_other(other);
      value_ = std::move(other.value_);
    }
    return *this;
  }
  virtual void lock() const = 0;
  virtual void unlock() const = 0;
  virtual bool try_lock() const = 0;
  element_type* unsafe_get() { return &value_; }
  const element_type* unsafe_get() const { return &value_; }
};

template<class T, class Mutex>
class Synchronized : public Synchronized<T, void> {
  typedef Synchronized<T, void> base_type;
  mutable Mutex mutex_;
public:
  typedef Mutex mutex_type;
  template<class... U>
  Synchronized(U&&... args) : base_type(std::forward<U>(args)...) { }
  SynchronizedPtr<T> get() { return *this; }
  SynchronizedPtr<const T> get() const { return *this; }
  void lock() const { return mutex_.lock(); }
  void unlock() const { return mutex_.unlock(); }
  bool try_lock() const { return mutex_.try_lock(); }
  mutex_type& mutex() { return mutex_; }
};

std::string::iterator split_ip_address(std::string& ip_address);

const char* get_cmd_option(char** begin, char** end, const std::string& option);

void create_log_dir_or_die(const char* log_file_name);

#endif

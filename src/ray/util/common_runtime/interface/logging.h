#ifndef RAY_UTIL_COMMON_RUNTIME_INTERFACE_LOGGING_H
#define RAY_UTIL_COMMON_RUNTIME_INTERFACE_LOGGING_H

#include <cstring>
#include <iostream>
#include <iterator>
#include <sstream>
#include <type_traits>

# ifndef _WIN32
#include <execinfo.h>
# endif

class CrLogger {
 public:
  CrLogger(int severity);
  virtual ~CrLogger();

  // default implementation for compound types
  template <typename T>
  typename std::enable_if<std::is_compound<T>::value, CrLogger>::type &operator<<(T val) {
    std::ostringstream ostream;
    ostream << val;
    return *this << ostream.str();
  }

  // unsigned integral
  template <typename UInt>
  typename std::enable_if<std::is_unsigned<UInt>::value, CrLogger>::type &operator<<(
      UInt val) {
    char buffer[20], *p = std::end(buffer);  // can hold uint64_t
    while (val) {
      *--p = '0' + val % 10;
      val /= 10;
    }
    this->write_string(p, std::end(buffer) - p);
    return *this;
  };

  // signed integral
  template <typename SInt>
  typename std::enable_if<
      std::__and_<std::is_signed<SInt>, std::is_integral<SInt>>::value, CrLogger>::type &
  operator<<(SInt val) {
    auto u = static_cast<typename std::make_unsigned<SInt>::type>(val);
    if (val < 0) {
      *this << '-';
      u = ~u + 1;
    }
    return *this << u;
  };

  // floating point
  template <typename F>
  typename std::enable_if<std::is_floating_point<F>::value, CrLogger>::type &operator<<(
      F val) {
    // 0.45x of sprintf.
    // suppose this is not critical; replace if needed.
    return *this << std::to_string(val);
  };

  CrLogger &operator<<(bool val) {
    if (val)
      this->write_string("true", strlen("true"));
    else
      this->write_string("false", strlen("false"));
    return *this;
  }

  CrLogger &operator<<(char val) {
    this->write_string(&val, 1);
    return *this;
  }

  CrLogger &operator<<(const char *str) {
    this->write_string(str, strlen(str));
    return *this;
  }

  CrLogger &operator<<(const std::string &str) {
    this->write_string(str.data(), str.length());
    return *this;
  }

  // vector, deque, list, set, multiset, unordered_set, unordered_multiset
  template <template <typename, typename...> class C, typename T, typename... Args>
  CrLogger &operator<<(const C<T, Args...> &collection) {
    auto iter = std::begin(collection), end = std::end(collection);
    *this << "[";
    if (iter != end) {
      *this << *iter++;
    }
    while (iter != end) {
      *this << ", " << *iter++;
    }
    *this << "]";
    return *this;
  }

  // map, multimap, unordered_map, unordered_multimap
  template <template <typename, typename, typename...> class C, typename K, typename V,
            typename... Args>
  typename std::enable_if<std::is_same<typename C<K, V, Args...>::mapped_type, V>::value,
                          CrLogger>::type &
  operator<<(const C<K, V, Args...> &dictionary) {
    auto iter = std::begin(dictionary), end = std::end(dictionary);
    *this << "{";
    if (iter != end) {
      *this << iter->first << ": " << iter->second;
      iter++;
    }
    while (iter != end) {
      *this << ", " << iter->first << ": " << iter->second;
      iter++;
    }
    *this << "}";
    return *this;
  }

 protected:
  // implemented by different providers
  void write_string(const char *str, size_t sz);

 protected:
  const int severity_;
  bool has_logged_;
};

class CrFatalLogger : public CrLogger {
 public:
  CrFatalLogger(int severity) : CrLogger(severity) {}
  virtual ~CrFatalLogger() 
  { 
    if (has_logged_) {
#if defined(_EXECINFO_H) || !defined(_WIN32)
      void *buffer[255];
      const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
      backtrace_symbols_fd(buffer, calls, 1);
#endif
    }
    std::abort();
  }
};

#endif  // RAY_UTIL_COMMON_RUNTIME_INTERFACE_LOGGING_H
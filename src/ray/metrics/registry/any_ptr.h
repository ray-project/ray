#ifndef RAY_METRICS_REGISTRY_ANY_PTR_H
#define RAY_METRICS_REGISTRY_ANY_PTR_H

#include <typeinfo>

namespace ray {

namespace metrics {

/// A container of any type of heap object, manage the life cycle of the object.
/// TODO(micafan) public function Swap
class AnyPtr {
 public:
  AnyPtr() : object_ptr_(nullptr), type_info_(&typeid(void *)), deleter_(nullptr) {}

  ~AnyPtr() { Destroy(); }

  // noncopyable
  AnyPtr(const AnyPtr &) = delete;
  AnyPtr &operator=(const AnyPtr &) = delete;

  /// Assign a heap object of type T.
  template <typename T>
  AnyPtr &operator=(T *object_ptr) {
    // Destroy old object before set new object.
    Destroy();

    object_ptr_ = object_ptr;
    type_info_ = &typeid(T *);
    deleter_ = &AnyPtr::Deleter<T>;
    return *this;
  }

  /// Cast object to T.
  template <typename T>
  T *CastTo() const {
    if (*type_info_ == typeid(T *) && object_ptr_ != nullptr) {
      return reinterpret_cast<T *>(object_ptr_);
    }

    return nullptr;
  }

 private:
  void Destroy() {
    if (object_ptr_ != nullptr && deleter_ != nullptr) {
      deleter_(object_ptr_);

      object_ptr_ = nullptr;
      deleter_ = nullptr;
      type_info_ = &typeid(void *);
    }
  }

  template <typename T>
  static void Deleter(void *ptr) {
    delete static_cast<T *>(ptr);
  }

 private:
  void *object_ptr_;
  const std::type_info *type_info_;
  void (*deleter_)(void *);
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REGISTRY_ANY_PTR_H

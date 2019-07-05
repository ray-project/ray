// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// optional.h
// -----------------------------------------------------------------------------
//
// This header file defines the `absl::optional` type for holding a value which
// may or may not be present. This type is useful for providing value semantics
// for operations that may either wish to return or hold "something-or-nothing".
//
// Example:
//
//   // A common way to signal operation failure is to provide an output
//   // parameter and a bool return type:
//   bool AcquireResource(const Input&, Resource * out);
//
//   // Providing an absl::optional return type provides a cleaner API:
//   absl::optional<Resource> AcquireResource(const Input&);
//
// `absl::optional` is a C++11 compatible version of the C++17 `std::optional`
// abstraction and is designed to be a drop-in replacement for code compliant
// with C++17.
#ifndef ABSL_TYPES_OPTIONAL_H_
#define ABSL_TYPES_OPTIONAL_H_

#include "absl/base/config.h"
#include "absl/utility/utility.h"

#ifdef ABSL_HAVE_STD_OPTIONAL

#include <optional>

namespace absl {
using std::bad_optional_access;
using std::optional;
using std::make_optional;
using std::nullopt_t;
using std::nullopt;
}  // namespace absl

#else  // ABSL_HAVE_STD_OPTIONAL

#include <cassert>
#include <functional>
#include <initializer_list>
#include <new>
#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/memory/memory.h"
#include "absl/meta/type_traits.h"
#include "absl/types/bad_optional_access.h"

// ABSL_OPTIONAL_USE_INHERITING_CONSTRUCTORS
//
// Inheriting constructors is supported in GCC 4.8+, Clang 3.3+ and MSVC 2015.
// __cpp_inheriting_constructors is a predefined macro and a recommended way to
// check for this language feature, but GCC doesn't support it until 5.0 and
// Clang doesn't support it until 3.6.
// Also, MSVC 2015 has a bug: it doesn't inherit the constexpr template
// constructor. For example, the following code won't work on MSVC 2015 Update3:
// struct Base {
//   int t;
//   template <typename T>
//   constexpr Base(T t_) : t(t_) {}
// };
// struct Foo : Base {
//   using Base::Base;
// }
// constexpr Foo foo(0);  // doesn't work on MSVC 2015
#if defined(__clang__)
#if __has_feature(cxx_inheriting_constructors)
#define ABSL_OPTIONAL_USE_INHERITING_CONSTRUCTORS 1
#endif
#elif (defined(__GNUC__) &&                                       \
       (__GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 8)) || \
    (__cpp_inheriting_constructors >= 200802) ||                  \
    (defined(_MSC_VER) && _MSC_VER >= 1910)
#define ABSL_OPTIONAL_USE_INHERITING_CONSTRUCTORS 1
#endif

namespace absl {

// -----------------------------------------------------------------------------
// absl::optional
// -----------------------------------------------------------------------------
//
// A value of type `absl::optional<T>` holds either a value of `T` or an
// "empty" value.  When it holds a value of `T`, it stores it as a direct
// sub-object, so `sizeof(optional<T>)` is approximately
// `sizeof(T) + sizeof(bool)`.
//
// This implementation is based on the specification in the latest draft of the
// C++17 `std::optional` specification as of May 2017, section 20.6.
//
// Differences between `absl::optional<T>` and `std::optional<T>` include:
//
//    * `constexpr` is not used for non-const member functions.
//      (dependency on some differences between C++11 and C++14.)
//    * `absl::nullopt` and `absl::in_place` are not declared `constexpr`. We
//      need the inline variable support in C++17 for external linkage.
//    * Throws `absl::bad_optional_access` instead of
//      `std::bad_optional_access`.
//    * `optional::swap()` and `absl::swap()` relies on
//      `std::is_(nothrow_)swappable()`, which has been introduced in C++17.
//      As a workaround, we assume `is_swappable()` is always `true`
//      and `is_nothrow_swappable()` is the same as `std::is_trivial()`.
//    * `make_optional()` cannot be declared `constexpr` due to the absence of
//      guaranteed copy elision.
//    * The move constructor's `noexcept` specification is stronger, i.e. if the
//      default allocator is non-throwing (via setting
//      `ABSL_ALLOCATOR_NOTHROW`), it evaluates to `noexcept(true)`, because
//      we assume
//       a) move constructors should only throw due to allocation failure and
//       b) if T's move constructor allocates, it uses the same allocation
//          function as the default allocator.
template <typename T>
class optional;

// nullopt_t
//
// Class type for `absl::nullopt` used to indicate an `absl::optional<T>` type
// that does not contain a value.
struct nullopt_t {
  struct init_t {};
  static init_t init;

  // It must not be default-constructible to avoid ambiguity for opt = {}.
  // Note the non-const reference, which is to eliminate ambiguity for code
  // like:
  //
  // struct S { int value; };
  //
  // void Test() {
  //   optional<S> opt;
  //   opt = {{}};
  // }
  explicit constexpr nullopt_t(init_t& /*unused*/) {}
};

// nullopt
//
// A tag constant of type `absl::nullopt_t` used to indicate an empty
// `absl::optional` in certain functions, such as construction or assignment.
extern const nullopt_t nullopt;

namespace optional_internal {

struct empty_struct {};
// This class stores the data in optional<T>.
// It is specialized based on whether T is trivially destructible.
// This is the specialization for non trivially destructible type.
template <typename T, bool unused = std::is_trivially_destructible<T>::value>
class optional_data_dtor_base {
  struct dummy_type {
    static_assert(sizeof(T) % sizeof(empty_struct) == 0, "");
    // Use an array to avoid GCC 6 placement-new warning.
    empty_struct data[sizeof(T) / sizeof(empty_struct)];
  };

 protected:
  // Whether there is data or not.
  bool engaged_;
  // Data storage
  union {
    dummy_type dummy_;
    T data_;
  };

  void destruct() noexcept {
    if (engaged_) {
      data_.~T();
      engaged_ = false;
    }
  }

  // dummy_ must be initialized for constexpr constructor.
  constexpr optional_data_dtor_base() noexcept : engaged_(false), dummy_{{}} {}

  template <typename... Args>
  constexpr explicit optional_data_dtor_base(in_place_t, Args&&... args)
      : engaged_(true), data_(absl::forward<Args>(args)...) {}

  ~optional_data_dtor_base() { destruct(); }
};

// Specialization for trivially destructible type.
template <typename T>
class optional_data_dtor_base<T, true> {
  struct dummy_type {
    static_assert(sizeof(T) % sizeof(empty_struct) == 0, "");
    // Use array to avoid GCC 6 placement-new warning.
    empty_struct data[sizeof(T) / sizeof(empty_struct)];
  };

 protected:
  // Whether there is data or not.
  bool engaged_;
  // Data storage
  union {
    dummy_type dummy_;
    T data_;
  };
  void destruct() noexcept { engaged_ = false; }

  // dummy_ must be initialized for constexpr constructor.
  constexpr optional_data_dtor_base() noexcept : engaged_(false), dummy_{{}} {}

  template <typename... Args>
  constexpr explicit optional_data_dtor_base(in_place_t, Args&&... args)
      : engaged_(true), data_(absl::forward<Args>(args)...) {}
};

template <typename T>
class optional_data_base : public optional_data_dtor_base<T> {
 protected:
  using base = optional_data_dtor_base<T>;
#if ABSL_OPTIONAL_USE_INHERITING_CONSTRUCTORS
  using base::base;
#else
  optional_data_base() = default;

  template <typename... Args>
  constexpr explicit optional_data_base(in_place_t t, Args&&... args)
      : base(t, absl::forward<Args>(args)...) {}
#endif

  template <typename... Args>
  void construct(Args&&... args) {
    // Use dummy_'s address to work around casting cv-qualified T* to void*.
    ::new (static_cast<void*>(&this->dummy_)) T(std::forward<Args>(args)...);
    this->engaged_ = true;
  }

  template <typename U>
  void assign(U&& u) {
    if (this->engaged_) {
      this->data_ = std::forward<U>(u);
    } else {
      construct(std::forward<U>(u));
    }
  }
};

// TODO(absl-team): Add another class using
// std::is_trivially_move_constructible trait when available to match
// http://cplusplus.github.io/LWG/lwg-defects.html#2900, for types that
// have trivial move but nontrivial copy.
// Also, we should be checking is_trivially_copyable here, which is not
// supported now, so we use is_trivially_* traits instead.
template <typename T,
          bool unused = absl::is_trivially_copy_constructible<T>::value&&
              absl::is_trivially_copy_assignable<typename std::remove_cv<
                  T>::type>::value&& std::is_trivially_destructible<T>::value>
class optional_data;

// Trivially copyable types
template <typename T>
class optional_data<T, true> : public optional_data_base<T> {
 protected:
#if ABSL_OPTIONAL_USE_INHERITING_CONSTRUCTORS
  using optional_data_base<T>::optional_data_base;
#else
  optional_data() = default;

  template <typename... Args>
  constexpr explicit optional_data(in_place_t t, Args&&... args)
      : optional_data_base<T>(t, absl::forward<Args>(args)...) {}
#endif
};

template <typename T>
class optional_data<T, false> : public optional_data_base<T> {
 protected:
#if ABSL_OPTIONAL_USE_INHERITING_CONSTRUCTORS
  using optional_data_base<T>::optional_data_base;
#else
  template <typename... Args>
  constexpr explicit optional_data(in_place_t t, Args&&... args)
      : optional_data_base<T>(t, absl::forward<Args>(args)...) {}
#endif

  optional_data() = default;

  optional_data(const optional_data& rhs) {
    if (rhs.engaged_) {
      this->construct(rhs.data_);
    }
  }

  optional_data(optional_data&& rhs) noexcept(
      absl::default_allocator_is_nothrow::value ||
      std::is_nothrow_move_constructible<T>::value) {
    if (rhs.engaged_) {
      this->construct(std::move(rhs.data_));
    }
  }

  optional_data& operator=(const optional_data& rhs) {
    if (rhs.engaged_) {
      this->assign(rhs.data_);
    } else {
      this->destruct();
    }
    return *this;
  }

  optional_data& operator=(optional_data&& rhs) noexcept(
      std::is_nothrow_move_assignable<T>::value&&
          std::is_nothrow_move_constructible<T>::value) {
    if (rhs.engaged_) {
      this->assign(std::move(rhs.data_));
    } else {
      this->destruct();
    }
    return *this;
  }
};

// Ordered by level of restriction, from low to high.
// Copyable implies movable.
enum class copy_traits { copyable = 0, movable = 1, non_movable = 2 };

// Base class for enabling/disabling copy/move constructor.
template <copy_traits>
class optional_ctor_base;

template <>
class optional_ctor_base<copy_traits::copyable> {
 public:
  constexpr optional_ctor_base() = default;
  optional_ctor_base(const optional_ctor_base&) = default;
  optional_ctor_base(optional_ctor_base&&) = default;
  optional_ctor_base& operator=(const optional_ctor_base&) = default;
  optional_ctor_base& operator=(optional_ctor_base&&) = default;
};

template <>
class optional_ctor_base<copy_traits::movable> {
 public:
  constexpr optional_ctor_base() = default;
  optional_ctor_base(const optional_ctor_base&) = delete;
  optional_ctor_base(optional_ctor_base&&) = default;
  optional_ctor_base& operator=(const optional_ctor_base&) = default;
  optional_ctor_base& operator=(optional_ctor_base&&) = default;
};

template <>
class optional_ctor_base<copy_traits::non_movable> {
 public:
  constexpr optional_ctor_base() = default;
  optional_ctor_base(const optional_ctor_base&) = delete;
  optional_ctor_base(optional_ctor_base&&) = delete;
  optional_ctor_base& operator=(const optional_ctor_base&) = default;
  optional_ctor_base& operator=(optional_ctor_base&&) = default;
};

// Base class for enabling/disabling copy/move assignment.
template <copy_traits>
class optional_assign_base;

template <>
class optional_assign_base<copy_traits::copyable> {
 public:
  constexpr optional_assign_base() = default;
  optional_assign_base(const optional_assign_base&) = default;
  optional_assign_base(optional_assign_base&&) = default;
  optional_assign_base& operator=(const optional_assign_base&) = default;
  optional_assign_base& operator=(optional_assign_base&&) = default;
};

template <>
class optional_assign_base<copy_traits::movable> {
 public:
  constexpr optional_assign_base() = default;
  optional_assign_base(const optional_assign_base&) = default;
  optional_assign_base(optional_assign_base&&) = default;
  optional_assign_base& operator=(const optional_assign_base&) = delete;
  optional_assign_base& operator=(optional_assign_base&&) = default;
};

template <>
class optional_assign_base<copy_traits::non_movable> {
 public:
  constexpr optional_assign_base() = default;
  optional_assign_base(const optional_assign_base&) = default;
  optional_assign_base(optional_assign_base&&) = default;
  optional_assign_base& operator=(const optional_assign_base&) = delete;
  optional_assign_base& operator=(optional_assign_base&&) = delete;
};

template <typename T>
constexpr copy_traits get_ctor_copy_traits() {
  return std::is_copy_constructible<T>::value
             ? copy_traits::copyable
             : std::is_move_constructible<T>::value ? copy_traits::movable
                                                    : copy_traits::non_movable;
}

template <typename T>
constexpr copy_traits get_assign_copy_traits() {
  return absl::is_copy_assignable<T>::value &&
                 std::is_copy_constructible<T>::value
             ? copy_traits::copyable
             : absl::is_move_assignable<T>::value &&
                       std::is_move_constructible<T>::value
                   ? copy_traits::movable
                   : copy_traits::non_movable;
}

// Whether T is constructible or convertible from optional<U>.
template <typename T, typename U>
struct is_constructible_convertible_from_optional
    : std::integral_constant<
          bool, std::is_constructible<T, optional<U>&>::value ||
                    std::is_constructible<T, optional<U>&&>::value ||
                    std::is_constructible<T, const optional<U>&>::value ||
                    std::is_constructible<T, const optional<U>&&>::value ||
                    std::is_convertible<optional<U>&, T>::value ||
                    std::is_convertible<optional<U>&&, T>::value ||
                    std::is_convertible<const optional<U>&, T>::value ||
                    std::is_convertible<const optional<U>&&, T>::value> {};

// Whether T is constructible or convertible or assignable from optional<U>.
template <typename T, typename U>
struct is_constructible_convertible_assignable_from_optional
    : std::integral_constant<
          bool, is_constructible_convertible_from_optional<T, U>::value ||
                    std::is_assignable<T&, optional<U>&>::value ||
                    std::is_assignable<T&, optional<U>&&>::value ||
                    std::is_assignable<T&, const optional<U>&>::value ||
                    std::is_assignable<T&, const optional<U>&&>::value> {};

// Helper function used by [optional.relops], [optional.comp_with_t],
// for checking whether an expression is convertible to bool.
bool convertible_to_bool(bool);

// Base class for std::hash<absl::optional<T>>:
// If std::hash<std::remove_const_t<T>> is enabled, it provides operator() to
// compute the hash; Otherwise, it is disabled.
// Reference N4659 23.14.15 [unord.hash].
template <typename T, typename = size_t>
struct optional_hash_base {
  optional_hash_base() = delete;
  optional_hash_base(const optional_hash_base&) = delete;
  optional_hash_base(optional_hash_base&&) = delete;
  optional_hash_base& operator=(const optional_hash_base&) = delete;
  optional_hash_base& operator=(optional_hash_base&&) = delete;
};

template <typename T>
struct optional_hash_base<T, decltype(std::hash<absl::remove_const_t<T> >()(
                                 std::declval<absl::remove_const_t<T> >()))> {
  using argument_type = absl::optional<T>;
  using result_type = size_t;
  size_t operator()(const absl::optional<T>& opt) const {
    if (opt) {
      return std::hash<absl::remove_const_t<T> >()(*opt);
    } else {
      return static_cast<size_t>(0x297814aaad196e6dULL);
    }
  }
};

}  // namespace optional_internal

// -----------------------------------------------------------------------------
// absl::optional class definition
// -----------------------------------------------------------------------------

template <typename T>
class optional : private optional_internal::optional_data<T>,
                 private optional_internal::optional_ctor_base<
                     optional_internal::get_ctor_copy_traits<T>()>,
                 private optional_internal::optional_assign_base<
                     optional_internal::get_assign_copy_traits<T>()> {
  using data_base = optional_internal::optional_data<T>;

 public:
  typedef T value_type;

  // Constructors

  // Constructs an `optional` holding an empty value, NOT a default constructed
  // `T`.
  constexpr optional() noexcept {}

  // Constructs an `optional` initialized with `nullopt` to hold an empty value.
  constexpr optional(nullopt_t) noexcept {}  // NOLINT(runtime/explicit)

  // Copy constructor, standard semantics
  optional(const optional& src) = default;

  // Move constructor, standard semantics
  optional(optional&& src) = default;

  // Constructs a non-empty `optional` direct-initialized value of type `T` from
  // the arguments `std::forward<Args>(args)...`  within the `optional`.
  // (The `in_place_t` is a tag used to indicate that the contained object
  // should be constructed in-place.)
  //
  // TODO(absl-team): Add std::is_constructible<T, Args&&...> SFINAE.
  template <typename... Args>
  constexpr explicit optional(in_place_t, Args&&... args)
      : data_base(in_place_t(), absl::forward<Args>(args)...) {}

  // Constructs a non-empty `optional` direct-initialized value of type `T` from
  // the arguments of an initializer_list and `std::forward<Args>(args)...`.
  // (The `in_place_t` is a tag used to indicate that the contained object
  // should be constructed in-place.)
  template <typename U, typename... Args,
            typename = typename std::enable_if<std::is_constructible<
                T, std::initializer_list<U>&, Args&&...>::value>::type>
  constexpr explicit optional(in_place_t, std::initializer_list<U> il,
                              Args&&... args)
      : data_base(in_place_t(), il, absl::forward<Args>(args)...) {
  }

  // Value constructor (implicit)
  template <
      typename U = T,
      typename std::enable_if<
          absl::conjunction<absl::negation<std::is_same<
                                in_place_t, typename std::decay<U>::type> >,
                            absl::negation<std::is_same<
                                optional<T>, typename std::decay<U>::type> >,
                            std::is_convertible<U&&, T>,
                            std::is_constructible<T, U&&> >::value,
          bool>::type = false>
  constexpr optional(U&& v) : data_base(in_place_t(), absl::forward<U>(v)) {}

  // Value constructor (explicit)
  template <
      typename U = T,
      typename std::enable_if<
          absl::conjunction<absl::negation<std::is_same<
                                in_place_t, typename std::decay<U>::type>>,
                            absl::negation<std::is_same<
                                optional<T>, typename std::decay<U>::type>>,
                            absl::negation<std::is_convertible<U&&, T>>,
                            std::is_constructible<T, U&&>>::value,
          bool>::type = false>
  explicit constexpr optional(U&& v)
      : data_base(in_place_t(), absl::forward<U>(v)) {}

  // Converting copy constructor (implicit)
  template <typename U,
            typename std::enable_if<
                absl::conjunction<
                    absl::negation<std::is_same<T, U> >,
                    std::is_constructible<T, const U&>,
                    absl::negation<
                        optional_internal::
                            is_constructible_convertible_from_optional<T, U> >,
                    std::is_convertible<const U&, T> >::value,
                bool>::type = false>
  optional(const optional<U>& rhs) {
    if (rhs) {
      this->construct(*rhs);
    }
  }

  // Converting copy constructor (explicit)
  template <typename U,
            typename std::enable_if<
                absl::conjunction<
                    absl::negation<std::is_same<T, U>>,
                    std::is_constructible<T, const U&>,
                    absl::negation<
                        optional_internal::
                            is_constructible_convertible_from_optional<T, U>>,
                    absl::negation<std::is_convertible<const U&, T>>>::value,
                bool>::type = false>
  explicit optional(const optional<U>& rhs) {
    if (rhs) {
      this->construct(*rhs);
    }
  }

  // Converting move constructor (implicit)
  template <typename U,
            typename std::enable_if<
                absl::conjunction<
                    absl::negation<std::is_same<T, U> >,
                    std::is_constructible<T, U&&>,
                    absl::negation<
                        optional_internal::
                            is_constructible_convertible_from_optional<T, U> >,
                    std::is_convertible<U&&, T> >::value,
                bool>::type = false>
  optional(optional<U>&& rhs) {
    if (rhs) {
      this->construct(std::move(*rhs));
    }
  }

  // Converting move constructor (explicit)
  template <
      typename U,
      typename std::enable_if<
          absl::conjunction<
              absl::negation<std::is_same<T, U>>, std::is_constructible<T, U&&>,
              absl::negation<
                  optional_internal::is_constructible_convertible_from_optional<
                      T, U>>,
              absl::negation<std::is_convertible<U&&, T>>>::value,
          bool>::type = false>
  explicit optional(optional<U>&& rhs) {
    if (rhs) {
      this->construct(std::move(*rhs));
    }
  }

  // Destructor. Trivial if `T` is trivially destructible.
  ~optional() = default;

  // Assignment Operators

  // Assignment from `nullopt`
  //
  // Example:
  //
  //   struct S { int value; };
  //   optional<S> opt = absl::nullopt;  // Could also use opt = { };
  optional& operator=(nullopt_t) noexcept {
    this->destruct();
    return *this;
  }

  // Copy assignment operator, standard semantics
  optional& operator=(const optional& src) = default;

  // Move assignment operator, standard semantics
  optional& operator=(optional&& src) = default;

  // Value assignment operators
  template <
      typename U = T,
      typename = typename std::enable_if<absl::conjunction<
          absl::negation<
              std::is_same<optional<T>, typename std::decay<U>::type>>,
          absl::negation<
              absl::conjunction<std::is_scalar<T>,
                                std::is_same<T, typename std::decay<U>::type>>>,
          std::is_constructible<T, U>, std::is_assignable<T&, U>>::value>::type>
  optional& operator=(U&& v) {
    this->assign(std::forward<U>(v));
    return *this;
  }

  template <
      typename U,
      typename = typename std::enable_if<absl::conjunction<
          absl::negation<std::is_same<T, U>>,
          std::is_constructible<T, const U&>, std::is_assignable<T&, const U&>,
          absl::negation<
              optional_internal::
                  is_constructible_convertible_assignable_from_optional<
                      T, U>>>::value>::type>
  optional& operator=(const optional<U>& rhs) {
    if (rhs) {
      this->assign(*rhs);
    } else {
      this->destruct();
    }
    return *this;
  }

  template <typename U,
            typename = typename std::enable_if<absl::conjunction<
                absl::negation<std::is_same<T, U>>, std::is_constructible<T, U>,
                std::is_assignable<T&, U>,
                absl::negation<
                    optional_internal::
                        is_constructible_convertible_assignable_from_optional<
                            T, U>>>::value>::type>
  optional& operator=(optional<U>&& rhs) {
    if (rhs) {
      this->assign(std::move(*rhs));
    } else {
      this->destruct();
    }
    return *this;
  }

  // Modifiers

  // optional::reset()
  //
  // Destroys the inner `T` value of an `absl::optional` if one is present.
  ABSL_ATTRIBUTE_REINITIALIZES void reset() noexcept { this->destruct(); }

  // optional::emplace()
  //
  // (Re)constructs the underlying `T` in-place with the given forwarded
  // arguments.
  //
  // Example:
  //
  //   optional<Foo> opt;
  //   opt.emplace(arg1,arg2,arg3);  // Constructs Foo(arg1,arg2,arg3)
  //
  // If the optional is non-empty, and the `args` refer to subobjects of the
  // current object, then behaviour is undefined, because the current object
  // will be destructed before the new object is constructed with `args`.
  template <typename... Args,
            typename = typename std::enable_if<
                std::is_constructible<T, Args&&...>::value>::type>
  T& emplace(Args&&... args) {
    this->destruct();
    this->construct(std::forward<Args>(args)...);
    return reference();
  }

  // Emplace reconstruction overload for an initializer list and the given
  // forwarded arguments.
  //
  // Example:
  //
  //   struct Foo {
  //     Foo(std::initializer_list<int>);
  //   };
  //
  //   optional<Foo> opt;
  //   opt.emplace({1,2,3});  // Constructs Foo({1,2,3})
  template <typename U, typename... Args,
            typename = typename std::enable_if<std::is_constructible<
                T, std::initializer_list<U>&, Args&&...>::value>::type>
  T& emplace(std::initializer_list<U> il, Args&&... args) {
    this->destruct();
    this->construct(il, std::forward<Args>(args)...);
    return reference();
  }

  // Swaps

  // Swap, standard semantics
  void swap(optional& rhs) noexcept(
      std::is_nothrow_move_constructible<T>::value&&
          std::is_trivial<T>::value) {
    if (*this) {
      if (rhs) {
        using std::swap;
        swap(**this, *rhs);
      } else {
        rhs.construct(std::move(**this));
        this->destruct();
      }
    } else {
      if (rhs) {
        this->construct(std::move(*rhs));
        rhs.destruct();
      } else {
        // No effect (swap(disengaged, disengaged)).
      }
    }
  }

  // Observers

  // optional::operator->()
  //
  // Accesses the underlying `T` value's member `m` of an `optional`. If the
  // `optional` is empty, behavior is undefined.
  //
  // If you need myOpt->foo in constexpr, use (*myOpt).foo instead.
  const T* operator->() const {
    assert(this->engaged_);
    return std::addressof(this->data_);
  }
  T* operator->() {
    assert(this->engaged_);
    return std::addressof(this->data_);
  }

  // optional::operator*()
  //
  // Accesses the underlying `T` value of an `optional`. If the `optional` is
  // empty, behavior is undefined.
  constexpr const T& operator*() const & { return reference(); }
  T& operator*() & {
    assert(this->engaged_);
    return reference();
  }
  constexpr const T&& operator*() const && {
    return absl::move(reference());
  }
  T&& operator*() && {
    assert(this->engaged_);
    return std::move(reference());
  }

  // optional::operator bool()
  //
  // Returns false if and only if the `optional` is empty.
  //
  //   if (opt) {
  //     // do something with opt.value();
  //   } else {
  //     // opt is empty.
  //   }
  //
  constexpr explicit operator bool() const noexcept { return this->engaged_; }

  // optional::has_value()
  //
  // Determines whether the `optional` contains a value. Returns `false` if and
  // only if `*this` is empty.
  constexpr bool has_value() const noexcept { return this->engaged_; }

// Suppress bogus warning on MSVC: MSVC complains call to reference() after
// throw_bad_optional_access() is unreachable.
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4702)
#endif  // _MSC_VER
  // optional::value()
  //
  // Returns a reference to an `optional`s underlying value. The constness
  // and lvalue/rvalue-ness of the `optional` is preserved to the view of
  // the `T` sub-object. Throws `absl::bad_optional_access` when the `optional`
  // is empty.
  constexpr const T& value() const & {
    return static_cast<bool>(*this)
               ? reference()
               : (optional_internal::throw_bad_optional_access(), reference());
  }
  T& value() & {
    return static_cast<bool>(*this)
               ? reference()
               : (optional_internal::throw_bad_optional_access(), reference());
  }
  T&& value() && {  // NOLINT(build/c++11)
    return std::move(
        static_cast<bool>(*this)
            ? reference()
            : (optional_internal::throw_bad_optional_access(), reference()));
  }
  constexpr const T&& value() const && {  // NOLINT(build/c++11)
    return absl::move(
        static_cast<bool>(*this)
            ? reference()
            : (optional_internal::throw_bad_optional_access(), reference()));
  }
#ifdef _MSC_VER
#pragma warning(pop)
#endif  // _MSC_VER

  // optional::value_or()
  //
  // Returns either the value of `T` or a passed default `v` if the `optional`
  // is empty.
  template <typename U>
  constexpr T value_or(U&& v) const& {
    static_assert(std::is_copy_constructible<value_type>::value,
                  "optional<T>::value_or: T must by copy constructible");
    static_assert(std::is_convertible<U&&, value_type>::value,
                  "optional<T>::value_or: U must be convertible to T");
    return static_cast<bool>(*this)
               ? **this
               : static_cast<T>(absl::forward<U>(v));
  }
  template <typename U>
  T value_or(U&& v) && {  // NOLINT(build/c++11)
    static_assert(std::is_move_constructible<value_type>::value,
                  "optional<T>::value_or: T must by copy constructible");
    static_assert(std::is_convertible<U&&, value_type>::value,
                  "optional<T>::value_or: U must be convertible to T");
    return static_cast<bool>(*this) ? std::move(**this)
                                    : static_cast<T>(std::forward<U>(v));
  }

 private:
  // Private accessors for internal storage viewed as reference to T.
  constexpr const T& reference() const { return this->data_; }
  T& reference() { return this->data_; }

  // T constraint checks.  You can't have an optional of nullopt_t, in_place_t
  // or a reference.
  static_assert(
      !std::is_same<nullopt_t, typename std::remove_cv<T>::type>::value,
      "optional<nullopt_t> is not allowed.");
  static_assert(
      !std::is_same<in_place_t, typename std::remove_cv<T>::type>::value,
      "optional<in_place_t> is not allowed.");
  static_assert(!std::is_reference<T>::value,
                "optional<reference> is not allowed.");
};

// Non-member functions

// swap()
//
// Performs a swap between two `absl::optional` objects, using standard
// semantics.
//
// NOTE: we assume `is_swappable()` is always `true`. A compile error will
// result if this is not the case.
template <typename T,
          typename std::enable_if<std::is_move_constructible<T>::value,
                                  bool>::type = false>
void swap(optional<T>& a, optional<T>& b) noexcept(noexcept(a.swap(b))) {
  a.swap(b);
}

// make_optional()
//
// Creates a non-empty `optional<T>` where the type of `T` is deduced. An
// `absl::optional` can also be explicitly instantiated with
// `make_optional<T>(v)`.
//
// Note: `make_optional()` constructions may be declared `constexpr` for
// trivially copyable types `T`. Non-trivial types require copy elision
// support in C++17 for `make_optional` to support `constexpr` on such
// non-trivial types.
//
// Example:
//
//   constexpr absl::optional<int> opt = absl::make_optional(1);
//   static_assert(opt.value() == 1, "");
template <typename T>
constexpr optional<typename std::decay<T>::type> make_optional(T&& v) {
  return optional<typename std::decay<T>::type>(absl::forward<T>(v));
}

template <typename T, typename... Args>
constexpr optional<T> make_optional(Args&&... args) {
  return optional<T>(in_place_t(), absl::forward<Args>(args)...);
}

template <typename T, typename U, typename... Args>
constexpr optional<T> make_optional(std::initializer_list<U> il,
                                    Args&&... args) {
  return optional<T>(in_place_t(), il,
                     absl::forward<Args>(args)...);
}

// Relational operators [optional.relops]

// Empty optionals are considered equal to each other and less than non-empty
// optionals. Supports relations between optional<T> and optional<U>, between
// optional<T> and U, and between optional<T> and nullopt.
//
// Note: We're careful to support T having non-bool relationals.

// Requires: The expression, e.g. "*x == *y" shall be well-formed and its result
// shall be convertible to bool.
// The C++17 (N4606) "Returns:" statements are translated into
// code in an obvious way here, and the original text retained as function docs.
// Returns: If bool(x) != bool(y), false; otherwise if bool(x) == false, true;
// otherwise *x == *y.
template <typename T, typename U>
constexpr auto operator==(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x == *y)) {
  return static_cast<bool>(x) != static_cast<bool>(y)
             ? false
             : static_cast<bool>(x) == false ? true
                                             : static_cast<bool>(*x == *y);
}

// Returns: If bool(x) != bool(y), true; otherwise, if bool(x) == false, false;
// otherwise *x != *y.
template <typename T, typename U>
constexpr auto operator!=(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x != *y)) {
  return static_cast<bool>(x) != static_cast<bool>(y)
             ? true
             : static_cast<bool>(x) == false ? false
                                             : static_cast<bool>(*x != *y);
}
// Returns: If !y, false; otherwise, if !x, true; otherwise *x < *y.
template <typename T, typename U>
constexpr auto operator<(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x < *y)) {
  return !y ? false : !x ? true : static_cast<bool>(*x < *y);
}
// Returns: If !x, false; otherwise, if !y, true; otherwise *x > *y.
template <typename T, typename U>
constexpr auto operator>(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x > *y)) {
  return !x ? false : !y ? true : static_cast<bool>(*x > *y);
}
// Returns: If !x, true; otherwise, if !y, false; otherwise *x <= *y.
template <typename T, typename U>
constexpr auto operator<=(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x <= *y)) {
  return !x ? true : !y ? false : static_cast<bool>(*x <= *y);
}
// Returns: If !y, true; otherwise, if !x, false; otherwise *x >= *y.
template <typename T, typename U>
constexpr auto operator>=(const optional<T>& x, const optional<U>& y)
    -> decltype(optional_internal::convertible_to_bool(*x >= *y)) {
  return !y ? true : !x ? false : static_cast<bool>(*x >= *y);
}

// Comparison with nullopt [optional.nullops]
// The C++17 (N4606) "Returns:" statements are used directly here.
template <typename T>
constexpr bool operator==(const optional<T>& x, nullopt_t) noexcept {
  return !x;
}
template <typename T>
constexpr bool operator==(nullopt_t, const optional<T>& x) noexcept {
  return !x;
}
template <typename T>
constexpr bool operator!=(const optional<T>& x, nullopt_t) noexcept {
  return static_cast<bool>(x);
}
template <typename T>
constexpr bool operator!=(nullopt_t, const optional<T>& x) noexcept {
  return static_cast<bool>(x);
}
template <typename T>
constexpr bool operator<(const optional<T>&, nullopt_t) noexcept {
  return false;
}
template <typename T>
constexpr bool operator<(nullopt_t, const optional<T>& x) noexcept {
  return static_cast<bool>(x);
}
template <typename T>
constexpr bool operator<=(const optional<T>& x, nullopt_t) noexcept {
  return !x;
}
template <typename T>
constexpr bool operator<=(nullopt_t, const optional<T>&) noexcept {
  return true;
}
template <typename T>
constexpr bool operator>(const optional<T>& x, nullopt_t) noexcept {
  return static_cast<bool>(x);
}
template <typename T>
constexpr bool operator>(nullopt_t, const optional<T>&) noexcept {
  return false;
}
template <typename T>
constexpr bool operator>=(const optional<T>&, nullopt_t) noexcept {
  return true;
}
template <typename T>
constexpr bool operator>=(nullopt_t, const optional<T>& x) noexcept {
  return !x;
}

// Comparison with T [optional.comp_with_t]

// Requires: The expression, e.g. "*x == v" shall be well-formed and its result
// shall be convertible to bool.
// The C++17 (N4606) "Equivalent to:" statements are used directly here.
template <typename T, typename U>
constexpr auto operator==(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x == v)) {
  return static_cast<bool>(x) ? static_cast<bool>(*x == v) : false;
}
template <typename T, typename U>
constexpr auto operator==(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v == *x)) {
  return static_cast<bool>(x) ? static_cast<bool>(v == *x) : false;
}
template <typename T, typename U>
constexpr auto operator!=(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x != v)) {
  return static_cast<bool>(x) ? static_cast<bool>(*x != v) : true;
}
template <typename T, typename U>
constexpr auto operator!=(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v != *x)) {
  return static_cast<bool>(x) ? static_cast<bool>(v != *x) : true;
}
template <typename T, typename U>
constexpr auto operator<(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x < v)) {
  return static_cast<bool>(x) ? static_cast<bool>(*x < v) : true;
}
template <typename T, typename U>
constexpr auto operator<(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v < *x)) {
  return static_cast<bool>(x) ? static_cast<bool>(v < *x) : false;
}
template <typename T, typename U>
constexpr auto operator<=(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x <= v)) {
  return static_cast<bool>(x) ? static_cast<bool>(*x <= v) : true;
}
template <typename T, typename U>
constexpr auto operator<=(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v <= *x)) {
  return static_cast<bool>(x) ? static_cast<bool>(v <= *x) : false;
}
template <typename T, typename U>
constexpr auto operator>(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x > v)) {
  return static_cast<bool>(x) ? static_cast<bool>(*x > v) : false;
}
template <typename T, typename U>
constexpr auto operator>(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v > *x)) {
  return static_cast<bool>(x) ? static_cast<bool>(v > *x) : true;
}
template <typename T, typename U>
constexpr auto operator>=(const optional<T>& x, const U& v)
    -> decltype(optional_internal::convertible_to_bool(*x >= v)) {
  return static_cast<bool>(x) ? static_cast<bool>(*x >= v) : false;
}
template <typename T, typename U>
constexpr auto operator>=(const U& v, const optional<T>& x)
    -> decltype(optional_internal::convertible_to_bool(v >= *x)) {
  return static_cast<bool>(x) ? static_cast<bool>(v >= *x) : true;
}

}  // namespace absl

namespace std {

// std::hash specialization for absl::optional.
template <typename T>
struct hash<absl::optional<T> >
    : absl::optional_internal::optional_hash_base<T> {};

}  // namespace std

#undef ABSL_OPTIONAL_USE_INHERITING_CONSTRUCTORS
#undef ABSL_MSVC_CONSTEXPR_BUG_IN_UNION_LIKE_CLASS

#endif  // ABSL_HAVE_STD_OPTIONAL

#endif  // ABSL_TYPES_OPTIONAL_H_

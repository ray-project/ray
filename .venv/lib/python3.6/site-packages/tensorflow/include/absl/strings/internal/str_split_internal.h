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

// This file declares INTERNAL parts of the Split API that are inline/templated
// or otherwise need to be available at compile time. The main abstractions
// defined in here are
//
//   - ConvertibleToStringView
//   - SplitIterator<>
//   - Splitter<>
//
// DO NOT INCLUDE THIS FILE DIRECTLY. Use this file by including
// absl/strings/str_split.h.
//
// IWYU pragma: private, include "absl/strings/str_split.h"

#ifndef ABSL_STRINGS_INTERNAL_STR_SPLIT_INTERNAL_H_
#define ABSL_STRINGS_INTERNAL_STR_SPLIT_INTERNAL_H_

#include <array>
#include <initializer_list>
#include <iterator>
#include <map>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/macros.h"
#include "absl/base/port.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"

#ifdef _GLIBCXX_DEBUG
#include "absl/strings/internal/stl_type_traits.h"
#endif  // _GLIBCXX_DEBUG

namespace absl {
namespace strings_internal {

// This class is implicitly constructible from everything that absl::string_view
// is implicitly constructible from. If it's constructed from a temporary
// string, the data is moved into a data member so its lifetime matches that of
// the ConvertibleToStringView instance.
class ConvertibleToStringView {
 public:
  ConvertibleToStringView(const char* s)  // NOLINT(runtime/explicit)
      : value_(s) {}
  ConvertibleToStringView(char* s) : value_(s) {}  // NOLINT(runtime/explicit)
  ConvertibleToStringView(absl::string_view s)     // NOLINT(runtime/explicit)
      : value_(s) {}
  ConvertibleToStringView(const std::string& s)  // NOLINT(runtime/explicit)
      : value_(s) {}

  // Matches rvalue strings and moves their data to a member.
ConvertibleToStringView(std::string&& s)  // NOLINT(runtime/explicit)
    : copy_(std::move(s)), value_(copy_) {}

  ConvertibleToStringView(const ConvertibleToStringView& other)
      : copy_(other.copy_),
        value_(other.IsSelfReferential() ? copy_ : other.value_) {}

  ConvertibleToStringView(ConvertibleToStringView&& other) {
    StealMembers(std::move(other));
  }

  ConvertibleToStringView& operator=(ConvertibleToStringView other) {
    StealMembers(std::move(other));
    return *this;
  }

  absl::string_view value() const { return value_; }

 private:
  // Returns true if ctsp's value refers to its internal copy_ member.
  bool IsSelfReferential() const { return value_.data() == copy_.data(); }

  void StealMembers(ConvertibleToStringView&& other) {
    if (other.IsSelfReferential()) {
      copy_ = std::move(other.copy_);
      value_ = copy_;
      other.value_ = other.copy_;
    } else {
      value_ = other.value_;
    }
  }

  // Holds the data moved from temporary std::string arguments. Declared first so
  // that 'value' can refer to 'copy_'.
  std::string copy_;
  absl::string_view value_;
};

// An iterator that enumerates the parts of a string from a Splitter. The text
// to be split, the Delimiter, and the Predicate are all taken from the given
// Splitter object. Iterators may only be compared if they refer to the same
// Splitter instance.
//
// This class is NOT part of the public splitting API.
template <typename Splitter>
class SplitIterator {
 public:
  using iterator_category = std::input_iterator_tag;
  using value_type = absl::string_view;
  using difference_type = ptrdiff_t;
  using pointer = const value_type*;
  using reference = const value_type&;

  enum State { kInitState, kLastState, kEndState };
  SplitIterator(State state, const Splitter* splitter)
      : pos_(0),
        state_(state),
        splitter_(splitter),
        delimiter_(splitter->delimiter()),
        predicate_(splitter->predicate()) {
    // Hack to maintain backward compatibility. This one block makes it so an
    // empty absl::string_view whose .data() happens to be nullptr behaves
    // *differently* from an otherwise empty absl::string_view whose .data() is
    // not nullptr. This is an undesirable difference in general, but this
    // behavior is maintained to avoid breaking existing code that happens to
    // depend on this old behavior/bug. Perhaps it will be fixed one day. The
    // difference in behavior is as follows:
    //   Split(absl::string_view(""), '-');  // {""}
    //   Split(absl::string_view(), '-');    // {}
    if (splitter_->text().data() == nullptr) {
      state_ = kEndState;
      pos_ = splitter_->text().size();
      return;
    }

    if (state_ == kEndState) {
      pos_ = splitter_->text().size();
    } else {
      ++(*this);
    }
  }

  bool at_end() const { return state_ == kEndState; }

  reference operator*() const { return curr_; }
  pointer operator->() const { return &curr_; }

  SplitIterator& operator++() {
    do {
      if (state_ == kLastState) {
        state_ = kEndState;
        return *this;
      }
      const absl::string_view text = splitter_->text();
      const absl::string_view d = delimiter_.Find(text, pos_);
      if (d.data() == text.data() + text.size()) state_ = kLastState;
      curr_ = text.substr(pos_, d.data() - (text.data() + pos_));
      pos_ += curr_.size() + d.size();
    } while (!predicate_(curr_));
    return *this;
  }

  SplitIterator operator++(int) {
    SplitIterator old(*this);
    ++(*this);
    return old;
  }

  friend bool operator==(const SplitIterator& a, const SplitIterator& b) {
    return a.state_ == b.state_ && a.pos_ == b.pos_;
  }

  friend bool operator!=(const SplitIterator& a, const SplitIterator& b) {
    return !(a == b);
  }

 private:
  size_t pos_;
  State state_;
  absl::string_view curr_;
  const Splitter* splitter_;
  typename Splitter::DelimiterType delimiter_;
  typename Splitter::PredicateType predicate_;
};

// HasMappedType<T>::value is true iff there exists a type T::mapped_type.
template <typename T, typename = void>
struct HasMappedType : std::false_type {};
template <typename T>
struct HasMappedType<T, absl::void_t<typename T::mapped_type>>
    : std::true_type {};

// HasValueType<T>::value is true iff there exists a type T::value_type.
template <typename T, typename = void>
struct HasValueType : std::false_type {};
template <typename T>
struct HasValueType<T, absl::void_t<typename T::value_type>> : std::true_type {
};

// HasConstIterator<T>::value is true iff there exists a type T::const_iterator.
template <typename T, typename = void>
struct HasConstIterator : std::false_type {};
template <typename T>
struct HasConstIterator<T, absl::void_t<typename T::const_iterator>>
    : std::true_type {};

// IsInitializerList<T>::value is true iff T is an std::initializer_list. More
// details below in Splitter<> where this is used.
std::false_type IsInitializerListDispatch(...);  // default: No
template <typename T>
std::true_type IsInitializerListDispatch(std::initializer_list<T>*);
template <typename T>
struct IsInitializerList
    : decltype(IsInitializerListDispatch(static_cast<T*>(nullptr))) {};

// A SplitterIsConvertibleTo<C>::type alias exists iff the specified condition
// is true for type 'C'.
//
// Restricts conversion to container-like types (by testing for the presence of
// a const_iterator member type) and also to disable conversion to an
// std::initializer_list (which also has a const_iterator). Otherwise, code
// compiled in C++11 will get an error due to ambiguous conversion paths (in
// C++11 std::vector<T>::operator= is overloaded to take either a std::vector<T>
// or an std::initializer_list<T>).

template <typename C, bool has_value_type, bool has_mapped_type>
struct SplitterIsConvertibleToImpl : std::false_type {};

template <typename C>
struct SplitterIsConvertibleToImpl<C, true, false>
    : std::is_constructible<typename C::value_type, absl::string_view> {};

template <typename C>
struct SplitterIsConvertibleToImpl<C, true, true>
    : absl::conjunction<
          std::is_constructible<typename C::key_type, absl::string_view>,
          std::is_constructible<typename C::mapped_type, absl::string_view>> {};

template <typename C>
struct SplitterIsConvertibleTo
    : SplitterIsConvertibleToImpl<
          C,
#ifdef _GLIBCXX_DEBUG
          !IsStrictlyBaseOfAndConvertibleToSTLContainer<C>::value &&
#endif  // _GLIBCXX_DEBUG
              !IsInitializerList<
                  typename std::remove_reference<C>::type>::value &&
              HasValueType<C>::value && HasConstIterator<C>::value,
          HasMappedType<C>::value> {
};

// This class implements the range that is returned by absl::StrSplit(). This
// class has templated conversion operators that allow it to be implicitly
// converted to a variety of types that the caller may have specified on the
// left-hand side of an assignment.
//
// The main interface for interacting with this class is through its implicit
// conversion operators. However, this class may also be used like a container
// in that it has .begin() and .end() member functions. It may also be used
// within a range-for loop.
//
// Output containers can be collections of any type that is constructible from
// an absl::string_view.
//
// An Predicate functor may be supplied. This predicate will be used to filter
// the split strings: only strings for which the predicate returns true will be
// kept. A Predicate object is any unary functor that takes an absl::string_view
// and returns bool.
template <typename Delimiter, typename Predicate>
class Splitter {
 public:
  using DelimiterType = Delimiter;
  using PredicateType = Predicate;
  using const_iterator = strings_internal::SplitIterator<Splitter>;
  using value_type = typename std::iterator_traits<const_iterator>::value_type;

  Splitter(ConvertibleToStringView input_text, Delimiter d, Predicate p)
      : text_(std::move(input_text)),
        delimiter_(std::move(d)),
        predicate_(std::move(p)) {}

  absl::string_view text() const { return text_.value(); }
  const Delimiter& delimiter() const { return delimiter_; }
  const Predicate& predicate() const { return predicate_; }

  // Range functions that iterate the split substrings as absl::string_view
  // objects. These methods enable a Splitter to be used in a range-based for
  // loop.
  const_iterator begin() const { return {const_iterator::kInitState, this}; }
  const_iterator end() const { return {const_iterator::kEndState, this}; }

  // An implicit conversion operator that is restricted to only those containers
  // that the splitter is convertible to.
  template <typename Container,
            typename = typename std::enable_if<
                SplitterIsConvertibleTo<Container>::value>::type>
  operator Container() const {  // NOLINT(runtime/explicit)
    return ConvertToContainer<Container, typename Container::value_type,
                              HasMappedType<Container>::value>()(*this);
  }

  // Returns a pair with its .first and .second members set to the first two
  // strings returned by the begin() iterator. Either/both of .first and .second
  // will be constructed with empty strings if the iterator doesn't have a
  // corresponding value.
  template <typename First, typename Second>
  operator std::pair<First, Second>() const {  // NOLINT(runtime/explicit)
    absl::string_view first, second;
    auto it = begin();
    if (it != end()) {
      first = *it;
      if (++it != end()) {
        second = *it;
      }
    }
    return {First(first), Second(second)};
  }

 private:
  // ConvertToContainer is a functor converting a Splitter to the requested
  // Container of ValueType. It is specialized below to optimize splitting to
  // certain combinations of Container and ValueType.
  //
  // This base template handles the generic case of storing the split results in
  // the requested non-map-like container and converting the split substrings to
  // the requested type.
  template <typename Container, typename ValueType, bool is_map = false>
  struct ConvertToContainer {
    Container operator()(const Splitter& splitter) const {
      Container c;
      auto it = std::inserter(c, c.end());
      for (const auto sp : splitter) {
        *it++ = ValueType(sp);
      }
      return c;
    }
  };

  // Partial specialization for a std::vector<absl::string_view>.
  //
  // Optimized for the common case of splitting to a
  // std::vector<absl::string_view>. In this case we first split the results to
  // a small array of absl::string_view on the stack, to reduce reallocations.
  template <typename A>
  struct ConvertToContainer<std::vector<absl::string_view, A>,
                            absl::string_view, false> {
    std::vector<absl::string_view, A> operator()(
        const Splitter& splitter) const {
      struct raw_view {
        const char* data;
        size_t size;
        operator absl::string_view() const {  // NOLINT(runtime/explicit)
          return {data, size};
        }
      };
      std::vector<absl::string_view, A> v;
      std::array<raw_view, 16> ar;
      for (auto it = splitter.begin(); !it.at_end();) {
        size_t index = 0;
        do {
          ar[index].data = it->data();
          ar[index].size = it->size();
          ++it;
        } while (++index != ar.size() && !it.at_end());
        v.insert(v.end(), ar.begin(), ar.begin() + index);
      }
      return v;
    }
  };

  // Partial specialization for a std::vector<std::string>.
  //
  // Optimized for the common case of splitting to a std::vector<std::string>. In
  // this case we first split the results to a std::vector<absl::string_view> so
  // the returned std::vector<std::string> can have space reserved to avoid std::string
  // moves.
  template <typename A>
  struct ConvertToContainer<std::vector<std::string, A>, std::string, false> {
    std::vector<std::string, A> operator()(const Splitter& splitter) const {
      const std::vector<absl::string_view> v = splitter;
      return std::vector<std::string, A>(v.begin(), v.end());
    }
  };

  // Partial specialization for containers of pairs (e.g., maps).
  //
  // The algorithm is to insert a new pair into the map for each even-numbered
  // item, with the even-numbered item as the key with a default-constructed
  // value. Each odd-numbered item will then be assigned to the last pair's
  // value.
  template <typename Container, typename First, typename Second>
  struct ConvertToContainer<Container, std::pair<const First, Second>, true> {
    Container operator()(const Splitter& splitter) const {
      Container m;
      typename Container::iterator it;
      bool insert = true;
      for (const auto sp : splitter) {
        if (insert) {
          it = Inserter<Container>::Insert(&m, First(sp), Second());
        } else {
          it->second = Second(sp);
        }
        insert = !insert;
      }
      return m;
    }

    // Inserts the key and value into the given map, returning an iterator to
    // the inserted item. Specialized for std::map and std::multimap to use
    // emplace() and adapt emplace()'s return value.
    template <typename Map>
    struct Inserter {
      using M = Map;
      template <typename... Args>
      static typename M::iterator Insert(M* m, Args&&... args) {
        return m->insert(std::make_pair(std::forward<Args>(args)...)).first;
      }
    };

    template <typename... Ts>
    struct Inserter<std::map<Ts...>> {
      using M = std::map<Ts...>;
      template <typename... Args>
      static typename M::iterator Insert(M* m, Args&&... args) {
        return m->emplace(std::make_pair(std::forward<Args>(args)...)).first;
      }
    };

    template <typename... Ts>
    struct Inserter<std::multimap<Ts...>> {
      using M = std::multimap<Ts...>;
      template <typename... Args>
      static typename M::iterator Insert(M* m, Args&&... args) {
        return m->emplace(std::make_pair(std::forward<Args>(args)...));
      }
    };
  };

  ConvertibleToStringView text_;
  Delimiter delimiter_;
  Predicate predicate_;
};

}  // namespace strings_internal
}  // namespace absl

#endif  // ABSL_STRINGS_INTERNAL_STR_SPLIT_INTERNAL_H_

// Copyright 2018 The Abseil Authors.
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
// File: inlined_vector.h
// -----------------------------------------------------------------------------
//
// This header file contains the declaration and definition of an "inlined
// vector" which behaves in an equivalent fashion to a `std::vector`, except
// that storage for small sequences of the vector are provided inline without
// requiring any heap allocation.
//
// An `absl::InlinedVector<T, N>` specifies the default capacity `N` as one of
// its template parameters. Instances where `size() <= N` hold contained
// elements in inline space. Typically `N` is very small so that sequences that
// are expected to be short do not require allocations.
//
// An `absl::InlinedVector` does not usually require a specific allocator. If
// the inlined vector grows beyond its initial constraints, it will need to
// allocate (as any normal `std::vector` would). This is usually performed with
// the default allocator (defined as `std::allocator<T>`). Optionally, a custom
// allocator type may be specified as `A` in `absl::InlinedVector<T, N, A>`.

#ifndef ABSL_CONTAINER_INLINED_VECTOR_H_
#define ABSL_CONTAINER_INLINED_VECTOR_H_

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>

#include "absl/algorithm/algorithm.h"
#include "absl/base/internal/throw_delegate.h"
#include "absl/base/optimization.h"
#include "absl/base/port.h"
#include "absl/memory/memory.h"

namespace absl {

// -----------------------------------------------------------------------------
// InlinedVector
// -----------------------------------------------------------------------------
//
// An `absl::InlinedVector` is designed to be a drop-in replacement for
// `std::vector` for use cases where the vector's size is sufficiently small
// that it can be inlined. If the inlined vector does grow beyond its estimated
// capacity, it will trigger an initial allocation on the heap, and will behave
// as a `std:vector`. The API of the `absl::InlinedVector` within this file is
// designed to cover the same API footprint as covered by `std::vector`.
template <typename T, size_t N, typename A = std::allocator<T>>
class InlinedVector {
  static_assert(N > 0, "InlinedVector requires inline capacity greater than 0");
  constexpr static typename A::size_type inlined_capacity() {
    return static_cast<typename A::size_type>(N);
  }

  template <typename Iterator>
  using DisableIfIntegral =
      absl::enable_if_t<!std::is_integral<Iterator>::value>;

  template <typename Iterator>
  using EnableIfInputIterator = absl::enable_if_t<std::is_convertible<
      typename std::iterator_traits<Iterator>::iterator_category,
      std::input_iterator_tag>::value>;

  template <typename Iterator>
  using IteratorCategory =
      typename std::iterator_traits<Iterator>::iterator_category;

  using rvalue_reference = typename A::value_type&&;

 public:
  using allocator_type = A;
  using value_type = typename allocator_type::value_type;
  using pointer = typename allocator_type::pointer;
  using const_pointer = typename allocator_type::const_pointer;
  using reference = typename allocator_type::reference;
  using const_reference = typename allocator_type::const_reference;
  using size_type = typename allocator_type::size_type;
  using difference_type = typename allocator_type::difference_type;
  using iterator = pointer;
  using const_iterator = const_pointer;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  // ---------------------------------------------------------------------------
  // InlinedVector Constructors and Destructor
  // ---------------------------------------------------------------------------

  // Creates an empty inlined vector with a default initialized allocator.
  InlinedVector() noexcept(noexcept(allocator_type()))
      : allocator_and_tag_(allocator_type()) {}

  // Creates an empty inlined vector with a specified allocator.
  explicit InlinedVector(const allocator_type& alloc) noexcept
      : allocator_and_tag_(alloc) {}

  // Creates an inlined vector with `n` copies of `value_type()`.
  explicit InlinedVector(size_type n,
                         const allocator_type& alloc = allocator_type())
      : allocator_and_tag_(alloc) {
    InitAssign(n);
  }

  // Creates an inlined vector with `n` copies of `v`.
  InlinedVector(size_type n, const_reference v,
                const allocator_type& alloc = allocator_type())
      : allocator_and_tag_(alloc) {
    InitAssign(n, v);
  }

  // Creates an inlined vector of copies of the values in `init_list`.
  InlinedVector(std::initializer_list<value_type> init_list,
                const allocator_type& alloc = allocator_type())
      : allocator_and_tag_(alloc) {
    AppendRange(init_list.begin(), init_list.end(),
                IteratorCategory<decltype(init_list.begin())>{});
  }

  // Creates an inlined vector with elements constructed from the provided
  // Iterator range [`first`, `last`).
  //
  // NOTE: The `enable_if` prevents ambiguous interpretation between a call to
  // this constructor with two integral arguments and a call to the above
  // `InlinedVector(size_type, const_reference)` constructor.
  template <typename InputIterator, DisableIfIntegral<InputIterator>* = nullptr>
  InlinedVector(InputIterator first, InputIterator last,
                const allocator_type& alloc = allocator_type())
      : allocator_and_tag_(alloc) {
    AppendRange(first, last, IteratorCategory<InputIterator>{});
  }

  // Creates a copy of `other` using `other`'s allocator.
  InlinedVector(const InlinedVector& other)
      : InlinedVector(other, other.get_allocator()) {}

  // Creates a copy of `other` but with a specified allocator.
  InlinedVector(const InlinedVector& other, const allocator_type& alloc)
      : allocator_and_tag_(alloc) {
    reserve(other.size());
    if (allocated()) {
      UninitializedCopy(other.begin(), other.end(), allocated_space());
      tag().set_allocated_size(other.size());
    } else {
      UninitializedCopy(other.begin(), other.end(), inlined_space());
      tag().set_inline_size(other.size());
    }
  }

  // Creates an inlined vector by moving in the contents of `other`.
  //
  // NOTE: This move constructor does not allocate and only moves the underlying
  // objects, so its `noexcept` specification depends on whether moving the
  // underlying objects can throw or not. We assume:
  //  a) move constructors should only throw due to allocation failure and
  //  b) if `value_type`'s move constructor allocates, it uses the same
  //     allocation function as the `InlinedVector`'s allocator, so the move
  //     constructor is non-throwing if the allocator is non-throwing or
  //     `value_type`'s move constructor is specified as `noexcept`.
  InlinedVector(InlinedVector&& other) noexcept(
      absl::allocator_is_nothrow<allocator_type>::value ||
      std::is_nothrow_move_constructible<value_type>::value)
      : allocator_and_tag_(other.allocator_and_tag_) {
    if (other.allocated()) {
      // We can just steal the underlying buffer from the source.
      // That leaves the source empty, so we clear its size.
      init_allocation(other.allocation());
      other.tag() = Tag();
    } else {
      UninitializedCopy(
          std::make_move_iterator(other.inlined_space()),
          std::make_move_iterator(other.inlined_space() + other.size()),
          inlined_space());
    }
  }

  // Creates an inlined vector by moving in the contents of `other`.
  //
  // NOTE: This move constructor allocates and subsequently moves the underlying
  // objects, so its `noexcept` specification depends on whether the allocation
  // can throw and whether moving the underlying objects can throw. Based on the
  // same assumptions as above, the `noexcept` specification is dominated by
  // whether the allocation can throw regardless of whether `value_type`'s move
  // constructor is specified as `noexcept`.
  InlinedVector(InlinedVector&& other, const allocator_type& alloc) noexcept(
      absl::allocator_is_nothrow<allocator_type>::value)
      : allocator_and_tag_(alloc) {
    if (other.allocated()) {
      if (alloc == other.allocator()) {
        // We can just steal the allocation from the source.
        tag() = other.tag();
        init_allocation(other.allocation());
        other.tag() = Tag();
      } else {
        // We need to use our own allocator
        reserve(other.size());
        UninitializedCopy(std::make_move_iterator(other.begin()),
                          std::make_move_iterator(other.end()),
                          allocated_space());
        tag().set_allocated_size(other.size());
      }
    } else {
      UninitializedCopy(
          std::make_move_iterator(other.inlined_space()),
          std::make_move_iterator(other.inlined_space() + other.size()),
          inlined_space());
      tag().set_inline_size(other.size());
    }
  }

  ~InlinedVector() { clear(); }

  // ---------------------------------------------------------------------------
  // InlinedVector Member Accessors
  // ---------------------------------------------------------------------------

  // `InlinedVector::empty()`
  //
  // Checks if the inlined vector has no elements.
  bool empty() const noexcept { return !size(); }

  // `InlinedVector::size()`
  //
  // Returns the number of elements in the inlined vector.
  size_type size() const noexcept { return tag().size(); }

  // `InlinedVector::max_size()`
  //
  // Returns the maximum number of elements the vector can hold.
  size_type max_size() const noexcept {
    // One bit of the size storage is used to indicate whether the inlined
    // vector is allocated. As a result, the maximum size of the container that
    // we can express is half of the max for `size_type`.
    return (std::numeric_limits<size_type>::max)() / 2;
  }

  // `InlinedVector::capacity()`
  //
  // Returns the number of elements that can be stored in the inlined vector
  // without requiring a reallocation of underlying memory.
  //
  // NOTE: For most inlined vectors, `capacity()` should equal
  // `inlined_capacity()`. For inlined vectors which exceed this capacity, they
  // will no longer be inlined and `capacity()` will equal its capacity on the
  // allocated heap.
  size_type capacity() const noexcept {
    return allocated() ? allocation().capacity() : inlined_capacity();
  }

  // `InlinedVector::data()`
  //
  // Returns a `pointer` to elements of the inlined vector. This pointer can be
  // used to access and modify the contained elements.
  // Only results within the range [`0`, `size()`) are defined.
  pointer data() noexcept {
    return allocated() ? allocated_space() : inlined_space();
  }

  // Overload of `InlinedVector::data()` to return a `const_pointer` to elements
  // of the inlined vector. This pointer can be used to access (but not modify)
  // the contained elements.
  const_pointer data() const noexcept {
    return allocated() ? allocated_space() : inlined_space();
  }

  // `InlinedVector::operator[]()`
  //
  // Returns a `reference` to the `i`th element of the inlined vector using the
  // array operator.
  reference operator[](size_type i) {
    assert(i < size());
    return data()[i];
  }

  // Overload of `InlinedVector::operator[]()` to return a `const_reference` to
  // the `i`th element of the inlined vector.
  const_reference operator[](size_type i) const {
    assert(i < size());
    return data()[i];
  }

  // `InlinedVector::at()`
  //
  // Returns a `reference` to the `i`th element of the inlined vector.
  reference at(size_type i) {
    if (ABSL_PREDICT_FALSE(i >= size())) {
      base_internal::ThrowStdOutOfRange(
          "`InlinedVector::at(size_type)` failed bounds check");
    }
    return data()[i];
  }

  // Overload of `InlinedVector::at()` to return a `const_reference` to the
  // `i`th element of the inlined vector.
  const_reference at(size_type i) const {
    if (ABSL_PREDICT_FALSE(i >= size())) {
      base_internal::ThrowStdOutOfRange(
          "`InlinedVector::at(size_type) const` failed bounds check");
    }
    return data()[i];
  }

  // `InlinedVector::front()`
  //
  // Returns a `reference` to the first element of the inlined vector.
  reference front() {
    assert(!empty());
    return at(0);
  }

  // Overload of `InlinedVector::front()` returns a `const_reference` to the
  // first element of the inlined vector.
  const_reference front() const {
    assert(!empty());
    return at(0);
  }

  // `InlinedVector::back()`
  //
  // Returns a `reference` to the last element of the inlined vector.
  reference back() {
    assert(!empty());
    return at(size() - 1);
  }

  // Overload of `InlinedVector::back()` to return a `const_reference` to the
  // last element of the inlined vector.
  const_reference back() const {
    assert(!empty());
    return at(size() - 1);
  }

  // `InlinedVector::begin()`
  //
  // Returns an `iterator` to the beginning of the inlined vector.
  iterator begin() noexcept { return data(); }

  // Overload of `InlinedVector::begin()` to return a `const_iterator` to
  // the beginning of the inlined vector.
  const_iterator begin() const noexcept { return data(); }

  // `InlinedVector::end()`
  //
  // Returns an `iterator` to the end of the inlined vector.
  iterator end() noexcept { return data() + size(); }

  // Overload of `InlinedVector::end()` to return a `const_iterator` to the
  // end of the inlined vector.
  const_iterator end() const noexcept { return data() + size(); }

  // `InlinedVector::cbegin()`
  //
  // Returns a `const_iterator` to the beginning of the inlined vector.
  const_iterator cbegin() const noexcept { return begin(); }

  // `InlinedVector::cend()`
  //
  // Returns a `const_iterator` to the end of the inlined vector.
  const_iterator cend() const noexcept { return end(); }

  // `InlinedVector::rbegin()`
  //
  // Returns a `reverse_iterator` from the end of the inlined vector.
  reverse_iterator rbegin() noexcept { return reverse_iterator(end()); }

  // Overload of `InlinedVector::rbegin()` to return a
  // `const_reverse_iterator` from the end of the inlined vector.
  const_reverse_iterator rbegin() const noexcept {
    return const_reverse_iterator(end());
  }

  // `InlinedVector::rend()`
  //
  // Returns a `reverse_iterator` from the beginning of the inlined vector.
  reverse_iterator rend() noexcept { return reverse_iterator(begin()); }

  // Overload of `InlinedVector::rend()` to return a `const_reverse_iterator`
  // from the beginning of the inlined vector.
  const_reverse_iterator rend() const noexcept {
    return const_reverse_iterator(begin());
  }

  // `InlinedVector::crbegin()`
  //
  // Returns a `const_reverse_iterator` from the end of the inlined vector.
  const_reverse_iterator crbegin() const noexcept { return rbegin(); }

  // `InlinedVector::crend()`
  //
  // Returns a `const_reverse_iterator` from the beginning of the inlined
  // vector.
  const_reverse_iterator crend() const noexcept { return rend(); }

  // `InlinedVector::get_allocator()`
  //
  // Returns a copy of the allocator of the inlined vector.
  allocator_type get_allocator() const { return allocator(); }

  // ---------------------------------------------------------------------------
  // InlinedVector Member Mutators
  // ---------------------------------------------------------------------------

  // `InlinedVector::operator=()`
  //
  // Replaces the contents of the inlined vector with copies of the elements in
  // the provided `std::initializer_list`.
  InlinedVector& operator=(std::initializer_list<value_type> init_list) {
    AssignRange(init_list.begin(), init_list.end(),
                IteratorCategory<decltype(init_list.begin())>{});
    return *this;
  }

  // Overload of `InlinedVector::operator=()` to replace the contents of the
  // inlined vector with the contents of `other`.
  InlinedVector& operator=(const InlinedVector& other) {
    if (ABSL_PREDICT_FALSE(this == &other)) return *this;

    // Optimized to avoid reallocation.
    // Prefer reassignment to copy construction for elements.
    if (size() < other.size()) {  // grow
      reserve(other.size());
      std::copy(other.begin(), other.begin() + size(), begin());
      std::copy(other.begin() + size(), other.end(), std::back_inserter(*this));
    } else {  // maybe shrink
      erase(begin() + other.size(), end());
      std::copy(other.begin(), other.end(), begin());
    }
    return *this;
  }

  // Overload of `InlinedVector::operator=()` to replace the contents of the
  // inlined vector with the contents of `other`.
  //
  // NOTE: As a result of calling this overload, `other` may be empty or it's
  // contents may be left in a moved-from state.
  InlinedVector& operator=(InlinedVector&& other) {
    if (ABSL_PREDICT_FALSE(this == &other)) return *this;

    if (other.allocated()) {
      clear();
      tag().set_allocated_size(other.size());
      init_allocation(other.allocation());
      other.tag() = Tag();
    } else {
      if (allocated()) clear();
      // Both are inlined now.
      if (size() < other.size()) {
        auto mid = std::make_move_iterator(other.begin() + size());
        std::copy(std::make_move_iterator(other.begin()), mid, begin());
        UninitializedCopy(mid, std::make_move_iterator(other.end()), end());
      } else {
        auto new_end = std::copy(std::make_move_iterator(other.begin()),
                                 std::make_move_iterator(other.end()), begin());
        Destroy(new_end, end());
      }
      tag().set_inline_size(other.size());
    }
    return *this;
  }

  // `InlinedVector::assign()`
  //
  // Replaces the contents of the inlined vector with `n` copies of `v`.
  void assign(size_type n, const_reference v) {
    if (n <= size()) {  // Possibly shrink
      std::fill_n(begin(), n, v);
      erase(begin() + n, end());
      return;
    }
    // Grow
    reserve(n);
    std::fill_n(begin(), size(), v);
    if (allocated()) {
      UninitializedFill(allocated_space() + size(), allocated_space() + n, v);
      tag().set_allocated_size(n);
    } else {
      UninitializedFill(inlined_space() + size(), inlined_space() + n, v);
      tag().set_inline_size(n);
    }
  }

  // Overload of `InlinedVector::assign()` to replace the contents of the
  // inlined vector with copies of the values in the provided
  // `std::initializer_list`.
  void assign(std::initializer_list<value_type> init_list) {
    AssignRange(init_list.begin(), init_list.end(),
                IteratorCategory<decltype(init_list.begin())>{});
  }

  // Overload of `InlinedVector::assign()` to replace the contents of the
  // inlined vector with values constructed from the range [`first`, `last`).
  template <typename InputIterator, DisableIfIntegral<InputIterator>* = nullptr>
  void assign(InputIterator first, InputIterator last) {
    AssignRange(first, last, IteratorCategory<InputIterator>{});
  }

  // `InlinedVector::resize()`
  //
  // Resizes the inlined vector to contain `n` elements. If `n` is smaller than
  // the inlined vector's current size, extra elements are destroyed. If `n` is
  // larger than the initial size, new elements are value-initialized.
  void resize(size_type n) {
    size_type s = size();
    if (n < s) {
      erase(begin() + n, end());
      return;
    }
    reserve(n);
    assert(capacity() >= n);

    // Fill new space with elements constructed in-place.
    if (allocated()) {
      UninitializedFill(allocated_space() + s, allocated_space() + n);
      tag().set_allocated_size(n);
    } else {
      UninitializedFill(inlined_space() + s, inlined_space() + n);
      tag().set_inline_size(n);
    }
  }

  // Overload of `InlinedVector::resize()` to resize the inlined vector to
  // contain `n` elements where, if `n` is larger than `size()`, the new values
  // will be copy-constructed from `v`.
  void resize(size_type n, const_reference v) {
    size_type s = size();
    if (n < s) {
      erase(begin() + n, end());
      return;
    }
    reserve(n);
    assert(capacity() >= n);

    // Fill new space with copies of `v`.
    if (allocated()) {
      UninitializedFill(allocated_space() + s, allocated_space() + n, v);
      tag().set_allocated_size(n);
    } else {
      UninitializedFill(inlined_space() + s, inlined_space() + n, v);
      tag().set_inline_size(n);
    }
  }

  // `InlinedVector::insert()`
  //
  // Copies `v` into `position`, returning an `iterator` pointing to the newly
  // inserted element.
  iterator insert(const_iterator position, const_reference v) {
    return emplace(position, v);
  }

  // Overload of `InlinedVector::insert()` for moving `v` into `position`,
  // returning an iterator pointing to the newly inserted element.
  iterator insert(const_iterator position, rvalue_reference v) {
    return emplace(position, std::move(v));
  }

  // Overload of `InlinedVector::insert()` for inserting `n` contiguous copies
  // of `v` starting at `position`. Returns an `iterator` pointing to the first
  // of the newly inserted elements.
  iterator insert(const_iterator position, size_type n, const_reference v) {
    return InsertWithCount(position, n, v);
  }

  // Overload of `InlinedVector::insert()` for copying the contents of the
  // `std::initializer_list` into the vector starting at `position`. Returns an
  // `iterator` pointing to the first of the newly inserted elements.
  iterator insert(const_iterator position,
                  std::initializer_list<value_type> init_list) {
    return insert(position, init_list.begin(), init_list.end());
  }

  // Overload of `InlinedVector::insert()` for inserting elements constructed
  // from the range [`first`, `last`). Returns an `iterator` pointing to the
  // first of the newly inserted elements.
  //
  // NOTE: The `enable_if` is intended to disambiguate the two three-argument
  // overloads of `insert()`.
  template <typename InputIterator,
            typename = EnableIfInputIterator<InputIterator>>
  iterator insert(const_iterator position, InputIterator first,
                  InputIterator last) {
    return InsertWithRange(position, first, last,
                           IteratorCategory<InputIterator>());
  }

  // `InlinedVector::emplace()`
  //
  // Constructs and inserts an object in the inlined vector at the given
  // `position`, returning an `iterator` pointing to the newly emplaced element.
  template <typename... Args>
  iterator emplace(const_iterator position, Args&&... args) {
    assert(position >= begin());
    assert(position <= end());
    if (ABSL_PREDICT_FALSE(position == end())) {
      emplace_back(std::forward<Args>(args)...);
      return end() - 1;
    }

    T new_t = T(std::forward<Args>(args)...);

    auto range = ShiftRight(position, 1);
    if (range.first == range.second) {
      // constructing into uninitialized memory
      Construct(range.first, std::move(new_t));
    } else {
      // assigning into moved-from object
      *range.first = T(std::move(new_t));
    }

    return range.first;
  }

  // `InlinedVector::emplace_back()`
  //
  // Constructs and appends a new element to the end of the inlined vector,
  // returning a `reference` to the emplaced element.
  template <typename... Args>
  reference emplace_back(Args&&... args) {
    size_type s = size();
    assert(s <= capacity());
    if (ABSL_PREDICT_FALSE(s == capacity())) {
      return GrowAndEmplaceBack(std::forward<Args>(args)...);
    }
    assert(s < capacity());

    pointer space;
    if (allocated()) {
      tag().set_allocated_size(s + 1);
      space = allocated_space();
    } else {
      tag().set_inline_size(s + 1);
      space = inlined_space();
    }
    return Construct(space + s, std::forward<Args>(args)...);
  }

  // `InlinedVector::push_back()`
  //
  // Appends a copy of `v` to the end of the inlined vector.
  void push_back(const_reference v) { static_cast<void>(emplace_back(v)); }

  // Overload of `InlinedVector::push_back()` for moving `v` into a newly
  // appended element.
  void push_back(rvalue_reference v) {
    static_cast<void>(emplace_back(std::move(v)));
  }

  // `InlinedVector::pop_back()`
  //
  // Destroys the element at the end of the inlined vector and shrinks the size
  // by `1` (unless the inlined vector is empty, in which case this is a no-op).
  void pop_back() noexcept {
    assert(!empty());
    size_type s = size();
    if (allocated()) {
      Destroy(allocated_space() + s - 1, allocated_space() + s);
      tag().set_allocated_size(s - 1);
    } else {
      Destroy(inlined_space() + s - 1, inlined_space() + s);
      tag().set_inline_size(s - 1);
    }
  }

  // `InlinedVector::erase()`
  //
  // Erases the element at `position` of the inlined vector, returning an
  // `iterator` pointing to the first element following the erased element.
  //
  // NOTE: May return the end iterator, which is not dereferencable.
  iterator erase(const_iterator position) {
    assert(position >= begin());
    assert(position < end());

    iterator pos = const_cast<iterator>(position);
    std::move(pos + 1, end(), pos);
    pop_back();
    return pos;
  }

  // Overload of `InlinedVector::erase()` for erasing all elements in the
  // range [`from`, `to`) in the inlined vector. Returns an `iterator` pointing
  // to the first element following the range erased or the end iterator if `to`
  // was the end iterator.
  iterator erase(const_iterator from, const_iterator to) {
    assert(begin() <= from);
    assert(from <= to);
    assert(to <= end());

    iterator range_start = const_cast<iterator>(from);
    iterator range_end = const_cast<iterator>(to);

    size_type s = size();
    ptrdiff_t erase_gap = std::distance(range_start, range_end);
    if (erase_gap > 0) {
      pointer space;
      if (allocated()) {
        space = allocated_space();
        tag().set_allocated_size(s - erase_gap);
      } else {
        space = inlined_space();
        tag().set_inline_size(s - erase_gap);
      }
      std::move(range_end, space + s, range_start);
      Destroy(space + s - erase_gap, space + s);
    }
    return range_start;
  }

  // `InlinedVector::clear()`
  //
  // Destroys all elements in the inlined vector, sets the size of `0` and
  // deallocates the heap allocation if the inlined vector was allocated.
  void clear() noexcept {
    size_type s = size();
    if (allocated()) {
      Destroy(allocated_space(), allocated_space() + s);
      allocation().Dealloc(allocator());
    } else if (s != 0) {  // do nothing for empty vectors
      Destroy(inlined_space(), inlined_space() + s);
    }
    tag() = Tag();
  }

  // `InlinedVector::reserve()`
  //
  // Enlarges the underlying representation of the inlined vector so it can hold
  // at least `n` elements. This method does not change `size()` or the actual
  // contents of the vector.
  //
  // NOTE: If `n` does not exceed `capacity()`, `reserve()` will have no
  // effects. Otherwise, `reserve()` will reallocate, performing an n-time
  // element-wise move of everything contained.
  void reserve(size_type n) {
    if (n > capacity()) {
      // Make room for new elements
      EnlargeBy(n - size());
    }
  }

  // `InlinedVector::shrink_to_fit()`
  //
  // Reduces memory usage by freeing unused memory. After this call, calls to
  // `capacity()` will be equal to `(std::max)(inlined_capacity(), size())`.
  //
  // If `size() <= inlined_capacity()` and the elements are currently stored on
  // the heap, they will be moved to the inlined storage and the heap memory
  // will be deallocated.
  //
  // If `size() > inlined_capacity()` and `size() < capacity()` the elements
  // will be moved to a smaller heap allocation.
  void shrink_to_fit() {
    const auto s = size();
    if (ABSL_PREDICT_FALSE(!allocated() || s == capacity())) return;

    if (s <= inlined_capacity()) {
      // Move the elements to the inlined storage.
      // We have to do this using a temporary, because `inlined_storage` and
      // `allocation_storage` are in a union field.
      auto temp = std::move(*this);
      assign(std::make_move_iterator(temp.begin()),
             std::make_move_iterator(temp.end()));
      return;
    }

    // Reallocate storage and move elements.
    // We can't simply use the same approach as above, because `assign()` would
    // call into `reserve()` internally and reserve larger capacity than we need
    Allocation new_allocation(allocator(), s);
    UninitializedCopy(std::make_move_iterator(allocated_space()),
                      std::make_move_iterator(allocated_space() + s),
                      new_allocation.buffer());
    ResetAllocation(new_allocation, s);
  }

  // `InlinedVector::swap()`
  //
  // Swaps the contents of this inlined vector with the contents of `other`.
  void swap(InlinedVector& other) {
    if (ABSL_PREDICT_FALSE(this == &other)) return;

    using std::swap;  // Augment ADL with `std::swap`.
    if (allocated() && other.allocated()) {
      // Both out of line, so just swap the tag, allocation, and allocator.
      swap(tag(), other.tag());
      swap(allocation(), other.allocation());
      swap(allocator(), other.allocator());
      return;
    }
    if (!allocated() && !other.allocated()) {
      // Both inlined: swap up to smaller size, then move remaining elements.
      InlinedVector* a = this;
      InlinedVector* b = &other;
      if (size() < other.size()) {
        swap(a, b);
      }

      const size_type a_size = a->size();
      const size_type b_size = b->size();
      assert(a_size >= b_size);
      // `a` is larger. Swap the elements up to the smaller array size.
      std::swap_ranges(a->inlined_space(), a->inlined_space() + b_size,
                       b->inlined_space());

      // Move the remaining elements:
      //   [`b_size`, `a_size`) from `a` -> [`b_size`, `a_size`) from `b`
      b->UninitializedCopy(a->inlined_space() + b_size,
                           a->inlined_space() + a_size,
                           b->inlined_space() + b_size);
      a->Destroy(a->inlined_space() + b_size, a->inlined_space() + a_size);

      swap(a->tag(), b->tag());
      swap(a->allocator(), b->allocator());
      assert(b->size() == a_size);
      assert(a->size() == b_size);
      return;
    }

    // One is out of line, one is inline.
    // We first move the elements from the inlined vector into the
    // inlined space in the other vector.  We then put the other vector's
    // pointer/capacity into the originally inlined vector and swap
    // the tags.
    InlinedVector* a = this;
    InlinedVector* b = &other;
    if (a->allocated()) {
      swap(a, b);
    }
    assert(!a->allocated());
    assert(b->allocated());
    const size_type a_size = a->size();
    const size_type b_size = b->size();
    // In an optimized build, `b_size` would be unused.
    static_cast<void>(b_size);

    // Made Local copies of `size()`, don't need `tag()` accurate anymore
    swap(a->tag(), b->tag());

    // Copy `b_allocation` out before `b`'s union gets clobbered by
    // `inline_space`
    Allocation b_allocation = b->allocation();

    b->UninitializedCopy(a->inlined_space(), a->inlined_space() + a_size,
                         b->inlined_space());
    a->Destroy(a->inlined_space(), a->inlined_space() + a_size);

    a->allocation() = b_allocation;

    if (a->allocator() != b->allocator()) {
      swap(a->allocator(), b->allocator());
    }

    assert(b->size() == a_size);
    assert(a->size() == b_size);
  }

 private:
  template <typename Hash, typename OtherT, size_t OtherN, typename OtherA>
  friend Hash AbslHashValue(Hash, const InlinedVector<OtherT, OtherN, OtherA>&);

  // Holds whether the vector is allocated or not in the lowest bit and the size
  // in the high bits:
  //   `size_ = (size << 1) | is_allocated;`
  class Tag {
   public:
    Tag() : size_(0) {}
    size_type size() const { return size_ / 2; }
    void add_size(size_type n) { size_ += n * 2; }
    void set_inline_size(size_type n) { size_ = n * 2; }
    void set_allocated_size(size_type n) { size_ = (n * 2) + 1; }
    bool allocated() const { return size_ % 2; }

   private:
    size_type size_;
  };

  // Derives from `allocator_type` to use the empty base class optimization.
  // If the `allocator_type` is stateless, we can store our instance for free.
  class AllocatorAndTag : private allocator_type {
   public:
    explicit AllocatorAndTag(const allocator_type& a) : allocator_type(a) {}

    Tag& tag() { return tag_; }
    const Tag& tag() const { return tag_; }

    allocator_type& allocator() { return *this; }
    const allocator_type& allocator() const { return *this; }

   private:
    Tag tag_;
  };

  class Allocation {
   public:
    Allocation(allocator_type& a, size_type capacity)
        : capacity_(capacity), buffer_(Create(a, capacity)) {}

    void Dealloc(allocator_type& a) {
      std::allocator_traits<allocator_type>::deallocate(a, buffer_, capacity_);
    }

    size_type capacity() const { return capacity_; }

    const_pointer buffer() const { return buffer_; }

    pointer buffer() { return buffer_; }

   private:
    static pointer Create(allocator_type& a, size_type n) {
      return std::allocator_traits<allocator_type>::allocate(a, n);
    }

    size_type capacity_;
    pointer buffer_;
  };

  const Tag& tag() const { return allocator_and_tag_.tag(); }

  Tag& tag() { return allocator_and_tag_.tag(); }

  Allocation& allocation() {
    return reinterpret_cast<Allocation&>(rep_.allocation_storage.allocation);
  }

  const Allocation& allocation() const {
    return reinterpret_cast<const Allocation&>(
        rep_.allocation_storage.allocation);
  }

  void init_allocation(const Allocation& allocation) {
    new (&rep_.allocation_storage.allocation) Allocation(allocation);
  }

  // TODO(absl-team): investigate whether the reinterpret_cast is appropriate.
  pointer inlined_space() {
    return reinterpret_cast<pointer>(
        std::addressof(rep_.inlined_storage.inlined[0]));
  }

  const_pointer inlined_space() const {
    return reinterpret_cast<const_pointer>(
        std::addressof(rep_.inlined_storage.inlined[0]));
  }

  pointer allocated_space() { return allocation().buffer(); }

  const_pointer allocated_space() const { return allocation().buffer(); }

  const allocator_type& allocator() const {
    return allocator_and_tag_.allocator();
  }

  allocator_type& allocator() { return allocator_and_tag_.allocator(); }

  bool allocated() const { return tag().allocated(); }

  void ResetAllocation(Allocation new_allocation, size_type new_size) {
    if (allocated()) {
      Destroy(allocated_space(), allocated_space() + size());
      assert(begin() == allocated_space());
      allocation().Dealloc(allocator());
      allocation() = new_allocation;
    } else {
      Destroy(inlined_space(), inlined_space() + size());
      init_allocation(new_allocation);  // bug: only init once
    }
    tag().set_allocated_size(new_size);
  }

  template <typename... Args>
  reference Construct(pointer p, Args&&... args) {
    std::allocator_traits<allocator_type>::construct(
        allocator(), p, std::forward<Args>(args)...);
    return *p;
  }

  template <typename Iterator>
  void UninitializedCopy(Iterator src, Iterator src_last, pointer dst) {
    for (; src != src_last; ++dst, ++src) Construct(dst, *src);
  }

  template <typename... Args>
  void UninitializedFill(pointer dst, pointer dst_last, const Args&... args) {
    for (; dst != dst_last; ++dst) Construct(dst, args...);
  }

  // Destroy [`from`, `to`) in place.
  void Destroy(pointer from, pointer to) {
    for (pointer cur = from; cur != to; ++cur) {
      std::allocator_traits<allocator_type>::destroy(allocator(), cur);
    }
#if !defined(NDEBUG)
    // Overwrite unused memory with `0xab` so we can catch uninitialized usage.
    // Cast to `void*` to tell the compiler that we don't care that we might be
    // scribbling on a vtable pointer.
    if (from != to) {
      auto len = sizeof(value_type) * std::distance(from, to);
      std::memset(reinterpret_cast<void*>(from), 0xab, len);
    }
#endif  // !defined(NDEBUG)
  }

  // Enlarge the underlying representation so we can store `size_ + delta` elems
  // in allocated space. The size is not changed, and any newly added memory is
  // not initialized.
  void EnlargeBy(size_type delta) {
    const size_type s = size();
    assert(s <= capacity());

    size_type target = (std::max)(inlined_capacity(), s + delta);

    // Compute new capacity by repeatedly doubling current capacity
    // TODO(psrc): Check and avoid overflow?
    size_type new_capacity = capacity();
    while (new_capacity < target) {
      new_capacity <<= 1;
    }

    Allocation new_allocation(allocator(), new_capacity);

    UninitializedCopy(std::make_move_iterator(data()),
                      std::make_move_iterator(data() + s),
                      new_allocation.buffer());

    ResetAllocation(new_allocation, s);
  }

  // Shift all elements from `position` to `end()` by `n` places to the right.
  // If the vector needs to be enlarged, memory will be allocated.
  // Returns `iterator`s pointing to the start of the previously-initialized
  // portion and the start of the uninitialized portion of the created gap.
  // The number of initialized spots is `pair.second - pair.first`. The number
  // of raw spots is `n - (pair.second - pair.first)`.
  //
  // Updates the size of the InlinedVector internally.
  std::pair<iterator, iterator> ShiftRight(const_iterator position,
                                           size_type n) {
    iterator start_used = const_cast<iterator>(position);
    iterator start_raw = const_cast<iterator>(position);
    size_type s = size();
    size_type required_size = s + n;

    if (required_size > capacity()) {
      // Compute new capacity by repeatedly doubling current capacity
      size_type new_capacity = capacity();
      while (new_capacity < required_size) {
        new_capacity <<= 1;
      }
      // Move everyone into the new allocation, leaving a gap of `n` for the
      // requested shift.
      Allocation new_allocation(allocator(), new_capacity);
      size_type index = position - begin();
      UninitializedCopy(std::make_move_iterator(data()),
                        std::make_move_iterator(data() + index),
                        new_allocation.buffer());
      UninitializedCopy(std::make_move_iterator(data() + index),
                        std::make_move_iterator(data() + s),
                        new_allocation.buffer() + index + n);
      ResetAllocation(new_allocation, s);

      // New allocation means our iterator is invalid, so we'll recalculate.
      // Since the entire gap is in new space, there's no used space to reuse.
      start_raw = begin() + index;
      start_used = start_raw;
    } else {
      // If we had enough space, it's a two-part move. Elements going into
      // previously-unoccupied space need an `UninitializedCopy()`. Elements
      // going into a previously-occupied space are just a `std::move()`.
      iterator pos = const_cast<iterator>(position);
      iterator raw_space = end();
      size_type slots_in_used_space = raw_space - pos;
      size_type new_elements_in_used_space = (std::min)(n, slots_in_used_space);
      size_type new_elements_in_raw_space = n - new_elements_in_used_space;
      size_type old_elements_in_used_space =
          slots_in_used_space - new_elements_in_used_space;

      UninitializedCopy(
          std::make_move_iterator(pos + old_elements_in_used_space),
          std::make_move_iterator(raw_space),
          raw_space + new_elements_in_raw_space);
      std::move_backward(pos, pos + old_elements_in_used_space, raw_space);

      // If the gap is entirely in raw space, the used space starts where the
      // raw space starts, leaving no elements in used space. If the gap is
      // entirely in used space, the raw space starts at the end of the gap,
      // leaving all elements accounted for within the used space.
      start_used = pos;
      start_raw = pos + new_elements_in_used_space;
    }
    tag().add_size(n);
    return std::make_pair(start_used, start_raw);
  }

  template <typename... Args>
  reference GrowAndEmplaceBack(Args&&... args) {
    assert(size() == capacity());
    const size_type s = size();

    Allocation new_allocation(allocator(), 2 * capacity());

    reference new_element =
        Construct(new_allocation.buffer() + s, std::forward<Args>(args)...);
    UninitializedCopy(std::make_move_iterator(data()),
                      std::make_move_iterator(data() + s),
                      new_allocation.buffer());

    ResetAllocation(new_allocation, s + 1);

    return new_element;
  }

  void InitAssign(size_type n) {
    if (n > inlined_capacity()) {
      Allocation new_allocation(allocator(), n);
      init_allocation(new_allocation);
      UninitializedFill(allocated_space(), allocated_space() + n);
      tag().set_allocated_size(n);
    } else {
      UninitializedFill(inlined_space(), inlined_space() + n);
      tag().set_inline_size(n);
    }
  }

  void InitAssign(size_type n, const_reference v) {
    if (n > inlined_capacity()) {
      Allocation new_allocation(allocator(), n);
      init_allocation(new_allocation);
      UninitializedFill(allocated_space(), allocated_space() + n, v);
      tag().set_allocated_size(n);
    } else {
      UninitializedFill(inlined_space(), inlined_space() + n, v);
      tag().set_inline_size(n);
    }
  }

  template <typename Iterator>
  void AssignRange(Iterator first, Iterator last, std::forward_iterator_tag) {
    auto length = std::distance(first, last);
    // Prefer reassignment to copy construction for elements.
    if (static_cast<size_type>(length) <= size()) {
      erase(std::copy(first, last, begin()), end());
      return;
    }
    reserve(length);
    iterator out = begin();
    for (; out != end(); ++first, ++out) *out = *first;
    if (allocated()) {
      UninitializedCopy(first, last, out);
      tag().set_allocated_size(length);
    } else {
      UninitializedCopy(first, last, out);
      tag().set_inline_size(length);
    }
  }

  template <typename Iterator>
  void AssignRange(Iterator first, Iterator last, std::input_iterator_tag) {
    // Optimized to avoid reallocation.
    // Prefer reassignment to copy construction for elements.
    iterator out = begin();
    for (; first != last && out != end(); ++first, ++out) {
      *out = *first;
    }
    erase(out, end());
    std::copy(first, last, std::back_inserter(*this));
  }

  template <typename Iterator>
  void AppendRange(Iterator first, Iterator last, std::forward_iterator_tag) {
    auto length = std::distance(first, last);
    reserve(size() + length);
    if (allocated()) {
      UninitializedCopy(first, last, allocated_space() + size());
      tag().set_allocated_size(size() + length);
    } else {
      UninitializedCopy(first, last, inlined_space() + size());
      tag().set_inline_size(size() + length);
    }
  }

  template <typename Iterator>
  void AppendRange(Iterator first, Iterator last, std::input_iterator_tag) {
    std::copy(first, last, std::back_inserter(*this));
  }

  iterator InsertWithCount(const_iterator position, size_type n,
                           const_reference v) {
    assert(position >= begin() && position <= end());
    if (ABSL_PREDICT_FALSE(n == 0)) return const_cast<iterator>(position);

    value_type copy = v;
    std::pair<iterator, iterator> it_pair = ShiftRight(position, n);
    std::fill(it_pair.first, it_pair.second, copy);
    UninitializedFill(it_pair.second, it_pair.first + n, copy);

    return it_pair.first;
  }

  template <typename ForwardIterator>
  iterator InsertWithRange(const_iterator position, ForwardIterator first,
                           ForwardIterator last, std::forward_iterator_tag) {
    assert(position >= begin() && position <= end());
    if (ABSL_PREDICT_FALSE(first == last))
      return const_cast<iterator>(position);

    auto n = std::distance(first, last);
    std::pair<iterator, iterator> it_pair = ShiftRight(position, n);
    size_type used_spots = it_pair.second - it_pair.first;
    ForwardIterator open_spot = std::next(first, used_spots);
    std::copy(first, open_spot, it_pair.first);
    UninitializedCopy(open_spot, last, it_pair.second);
    return it_pair.first;
  }

  template <typename InputIterator>
  iterator InsertWithRange(const_iterator position, InputIterator first,
                           InputIterator last, std::input_iterator_tag) {
    assert(position >= begin() && position <= end());
    size_type index = position - cbegin();
    size_type i = index;
    while (first != last) insert(begin() + i++, *first++);
    return begin() + index;
  }

  // Stores either the inlined or allocated representation
  union Rep {
    using ValueTypeBuffer =
        absl::aligned_storage_t<sizeof(value_type), alignof(value_type)>;
    using AllocationBuffer =
        absl::aligned_storage_t<sizeof(Allocation), alignof(Allocation)>;

    // Structs wrap the buffers to perform indirection that solves a bizarre
    // compilation error on Visual Studio (all known versions).
    struct InlinedRep {
      ValueTypeBuffer inlined[N];
    };
    struct AllocatedRep {
      AllocationBuffer allocation;
    };

    InlinedRep inlined_storage;
    AllocatedRep allocation_storage;
  };

  AllocatorAndTag allocator_and_tag_;
  Rep rep_;
};

// -----------------------------------------------------------------------------
// InlinedVector Non-Member Functions
// -----------------------------------------------------------------------------

// `swap()`
//
// Swaps the contents of two inlined vectors. This convenience function
// simply calls `InlinedVector::swap()`.
template <typename T, size_t N, typename A>
void swap(InlinedVector<T, N, A>& a,
          InlinedVector<T, N, A>& b) noexcept(noexcept(a.swap(b))) {
  a.swap(b);
}

// `operator==()`
//
// Tests the equivalency of the contents of two inlined vectors.
template <typename T, size_t N, typename A>
bool operator==(const InlinedVector<T, N, A>& a,
                const InlinedVector<T, N, A>& b) {
  return absl::equal(a.begin(), a.end(), b.begin(), b.end());
}

// `operator!=()`
//
// Tests the inequality of the contents of two inlined vectors.
template <typename T, size_t N, typename A>
bool operator!=(const InlinedVector<T, N, A>& a,
                const InlinedVector<T, N, A>& b) {
  return !(a == b);
}

// `operator<()`
//
// Tests whether the contents of one inlined vector are less than the contents
// of another through a lexicographical comparison operation.
template <typename T, size_t N, typename A>
bool operator<(const InlinedVector<T, N, A>& a,
               const InlinedVector<T, N, A>& b) {
  return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

// `operator>()`
//
// Tests whether the contents of one inlined vector are greater than the
// contents of another through a lexicographical comparison operation.
template <typename T, size_t N, typename A>
bool operator>(const InlinedVector<T, N, A>& a,
               const InlinedVector<T, N, A>& b) {
  return b < a;
}

// `operator<=()`
//
// Tests whether the contents of one inlined vector are less than or equal to
// the contents of another through a lexicographical comparison operation.
template <typename T, size_t N, typename A>
bool operator<=(const InlinedVector<T, N, A>& a,
                const InlinedVector<T, N, A>& b) {
  return !(b < a);
}

// `operator>=()`
//
// Tests whether the contents of one inlined vector are greater than or equal to
// the contents of another through a lexicographical comparison operation.
template <typename T, size_t N, typename A>
bool operator>=(const InlinedVector<T, N, A>& a,
                const InlinedVector<T, N, A>& b) {
  return !(a < b);
}

template <typename Hash, typename T, size_t N, typename A>
Hash AbslHashValue(Hash hash, const InlinedVector<T, N, A>& inlined_vector) {
  auto p = inlined_vector.data();
  auto n = inlined_vector.size();
  return Hash::combine(Hash::combine_contiguous(std::move(hash), p, n), n);
}

// -----------------------------------------------------------------------------
// Implementation of InlinedVector
//
// Do not depend on any below implementation details!
// -----------------------------------------------------------------------------

}  // namespace absl

#endif  // ABSL_CONTAINER_INLINED_VECTOR_H_

/**
 * MIT License
 * 
 * Copyright (c) 2017 Tessil
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#ifndef TSL_ORDERED_SET_H
#define TSL_ORDERED_SET_H


#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <initializer_list>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>
#include "ordered_hash.h"


namespace tsl {


/**
 * Implementation of an hash set using open adressing with robin hood with backshift delete to resolve collisions.
 * 
 * The particularity of this hash set is that it remembers the order in which the elements were added and
 * provide a way to access the structure which stores these values through the 'values_container()' method. 
 * The used container is defined by ValueTypeContainer, by default a std::deque is used (grows faster) but
 * a std::vector may be used. In this case the set provides a 'data()' method which give a direct access 
 * to the memory used to store the values (which can be usefull to communicate with C API's).
 * 
 * The Key must be copy constructible and/or move constructible. To use `unordered_erase` it also must be swappable.
 * 
 * The behaviour of the hash set is undefinded if the destructor of Key throws an exception.
 * 
 * By default the maximum size of a set is limited to 2^32 - 1 values, if needed this can be changed through
 * the IndexType template parameter. Using an `uint64_t` will raise this limit to 2^64 - 1 values but each
 * bucket will use 16 bytes instead of 8 bytes in addition to the space needed to store the values.
 * 
 * Iterators invalidation:
 *  - clear, operator=, reserve, rehash: always invalidate the iterators (also invalidate end()).
 *  - insert, emplace, emplace_hint, operator[]: when a std::vector is used as ValueTypeContainer 
 *                                               and if size() < capacity(), only end(). 
 *                                               Otherwise all the iterators are invalidated if an insert occurs.
 *  - erase, unordered_erase: when a std::vector is used as ValueTypeContainer invalidate the iterator of 
 *                            the erased element and all the ones after the erased element (including end()). 
 *                            Otherwise all the iterators are invalidated if an erase occurs.
 */
template<class Key, 
         class Hash = std::hash<Key>,
         class KeyEqual = std::equal_to<Key>,
         class Allocator = std::allocator<Key>,
         class ValueTypeContainer = std::deque<Key, Allocator>,
         class IndexType = std::uint_least32_t>
class ordered_set {
private:
    template<typename U>
    using has_is_transparent = tsl::detail_ordered_hash::has_is_transparent<U>;
    
    class KeySelect {
    public:
        using key_type = Key;
        
        const key_type& operator()(const Key& key) const noexcept {
            return key;
        }
        
        key_type& operator()(Key& key) noexcept {
            return key;
        }
    };
    
    using ht = detail_ordered_hash::ordered_hash<Key, KeySelect, void,
                                                 Hash, KeyEqual, Allocator, ValueTypeContainer, IndexType>;
            
public:
    using key_type = typename ht::key_type;
    using value_type = typename ht::value_type;
    using size_type = typename ht::size_type;
    using difference_type = typename ht::difference_type;
    using hasher = typename ht::hasher;
    using key_equal = typename ht::key_equal;
    using allocator_type = typename ht::allocator_type;
    using reference = typename ht::reference;
    using const_reference = typename ht::const_reference;
    using pointer = typename ht::pointer;
    using const_pointer = typename ht::const_pointer;
    using iterator = typename ht::iterator;
    using const_iterator = typename ht::const_iterator;
    using reverse_iterator = typename ht::reverse_iterator;
    using const_reverse_iterator = typename ht::const_reverse_iterator;
    
    using values_container_type = typename ht::values_container_type;

    
    /*
     * Constructors
     */
    ordered_set(): ordered_set(ht::DEFAULT_INIT_BUCKETS_SIZE) {
    }
    
    explicit ordered_set(size_type bucket_count, 
                         const Hash& hash = Hash(),
                         const KeyEqual& equal = KeyEqual(),
                         const Allocator& alloc = Allocator()): 
                        m_ht(bucket_count, hash, equal, alloc, ht::DEFAULT_MAX_LOAD_FACTOR)
    {
    }
    
    ordered_set(size_type bucket_count,
                const Allocator& alloc): ordered_set(bucket_count, Hash(), KeyEqual(), alloc)
    {
    }
    
    ordered_set(size_type bucket_count,
                const Hash& hash,
                const Allocator& alloc): ordered_set(bucket_count, hash, KeyEqual(), alloc)
    {
    }
    
    explicit ordered_set(const Allocator& alloc): ordered_set(ht::DEFAULT_INIT_BUCKETS_SIZE, alloc) {
    }
    
    template<class InputIt>
    ordered_set(InputIt first, InputIt last,
                size_type bucket_count = ht::DEFAULT_INIT_BUCKETS_SIZE,
                const Hash& hash = Hash(),
                const KeyEqual& equal = KeyEqual(),
                const Allocator& alloc = Allocator()): ordered_set(bucket_count, hash, equal, alloc)
    {
        insert(first, last);
    }
    
    template<class InputIt>
    ordered_set(InputIt first, InputIt last,
                size_type bucket_count,
                const Allocator& alloc): ordered_set(first, last, bucket_count, Hash(), KeyEqual(), alloc)
    {
    }
    
    template<class InputIt>
    ordered_set(InputIt first, InputIt last,
                size_type bucket_count,
                const Hash& hash,
                const Allocator& alloc): ordered_set(first, last, bucket_count, hash, KeyEqual(), alloc)
    {
    }

    ordered_set(std::initializer_list<value_type> init,
                size_type bucket_count = ht::DEFAULT_INIT_BUCKETS_SIZE,
                const Hash& hash = Hash(),
                const KeyEqual& equal = KeyEqual(),
                const Allocator& alloc = Allocator()): 
            ordered_set(init.begin(), init.end(), bucket_count, hash, equal, alloc)
    {
    }

    ordered_set(std::initializer_list<value_type> init,
                size_type bucket_count,
                const Allocator& alloc): 
            ordered_set(init.begin(), init.end(), bucket_count, Hash(), KeyEqual(), alloc)
    {
    }

    ordered_set(std::initializer_list<value_type> init,
                size_type bucket_count,
                const Hash& hash,
                const Allocator& alloc): 
            ordered_set(init.begin(), init.end(), bucket_count, hash, KeyEqual(), alloc)
    {
    }

    
    ordered_set& operator=(std::initializer_list<value_type> ilist) {
        m_ht.clear();
        
        m_ht.reserve(ilist.size());
        m_ht.insert(ilist.begin(), ilist.end());
        
        return *this;
    }
    
    allocator_type get_allocator() const { return m_ht.get_allocator(); }
    
    
    /*
     * Iterators
     */
    iterator begin() noexcept { return m_ht.begin(); }
    const_iterator begin() const noexcept { return m_ht.begin(); }
    const_iterator cbegin() const noexcept { return m_ht.cbegin(); }
    
    iterator end() noexcept { return m_ht.end(); }
    const_iterator end() const noexcept { return m_ht.end(); }
    const_iterator cend() const noexcept { return m_ht.cend(); }
    
    reverse_iterator rbegin() noexcept { return m_ht.rbegin(); }
    const_reverse_iterator rbegin() const noexcept { return m_ht.rbegin(); }
    const_reverse_iterator rcbegin() const noexcept { return m_ht.rcbegin(); }
    
    reverse_iterator rend() noexcept { return m_ht.rend(); }
    const_reverse_iterator rend() const noexcept { return m_ht.rend(); }
    const_reverse_iterator rcend() const noexcept { return m_ht.rcend(); }
    
    
    /*
     * Capacity
     */
    bool empty() const noexcept { return m_ht.empty(); }
    size_type size() const noexcept { return m_ht.size(); }
    size_type max_size() const noexcept { return m_ht.max_size(); }
    
    /*
     * Modifiers
     */
    void clear() noexcept { m_ht.clear(); }
    
    
    
    std::pair<iterator, bool> insert(const value_type& value) { return m_ht.insert(value); }
    std::pair<iterator, bool> insert(value_type&& value) { return m_ht.insert(std::move(value)); }
    
    iterator insert(const_iterator hint, const value_type& value) {
        return m_ht.insert(hint, value); 
    }
    
    iterator insert(const_iterator hint, value_type&& value) { 
        return m_ht.insert(hint, std::move(value)); 
    }
    
    template<class InputIt>
    void insert(InputIt first, InputIt last) { m_ht.insert(first, last); }
    void insert(std::initializer_list<value_type> ilist) { m_ht.insert(ilist.begin(), ilist.end()); }

    
    
    /**
     * Due to the way elements are stored, emplace will need to move or copy the key-value once.
     * The method is equivalent to insert(value_type(std::forward<Args>(args)...));
     * 
     * Mainly here for compatibility with the std::unordered_map interface.
     */
    template<class... Args>
    std::pair<iterator, bool> emplace(Args&&... args) { return m_ht.emplace(std::forward<Args>(args)...); }
    
    /**
     * Due to the way elements are stored, emplace_hint will need to move or copy the key-value once.
     * The method is equivalent to insert(hint, value_type(std::forward<Args>(args)...));
     * 
     * Mainly here for compatibility with the std::unordered_map interface.
     */
    template<class... Args>
    iterator emplace_hint(const_iterator hint, Args&&... args) {
        return m_ht.emplace_hint(hint, std::forward<Args>(args)...); 
    }

    /**
     * When erasing an element, the insert order will be preserved and no holes will be present in the container
     * returned by 'values_container()'. 
     * 
     * The method is in O(n), if the order is not important 'unordered_erase(...)' method is faster with an O(1)
     * average complexity.
     */    
    iterator erase(iterator pos) { return m_ht.erase(pos); }
    
    /**
     * @copydoc erase(iterator pos)
     */    
    iterator erase(const_iterator pos) { return m_ht.erase(pos); }
    
    /**
     * @copydoc erase(iterator pos)
     */    
    iterator erase(const_iterator first, const_iterator last) { return m_ht.erase(first, last); }
    
    /**
     * @copydoc erase(iterator pos)
     */    
    size_type erase(const key_type& key) { return m_ht.erase(key); }
    
    /**
     * @copydoc erase(iterator pos)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup to the value if you already have the hash.
     */    
    size_type erase(const key_type& key, std::size_t precalculated_hash) { 
        return m_ht.erase(key, precalculated_hash); 
    }
    
    /**
     * @copydoc erase(iterator pos)
     * 
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type erase(const K& key) { return m_ht.erase(key); }
    
    /**
     * @copydoc erase(const key_type& key, std::size_t precalculated_hash)
     * 
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type erase(const K& key, std::size_t precalculated_hash) { 
        return m_ht.erase(key, precalculated_hash); 
    }
    
    
    
    void swap(ordered_set& other) { other.m_ht.swap(m_ht); }
    
    /*
     * Lookup
     */
    size_type count(const Key& key) const { return m_ht.count(key); }
    
    /**
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    size_type count(const Key& key, std::size_t precalculated_hash) const { 
        return m_ht.count(key, precalculated_hash); 
    }
    
    /**
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr>
    size_type count(const K& key) const { return m_ht.count(key); }
    
    /**
     * @copydoc count(const K& key) const
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */     
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type count(const K& key, std::size_t precalculated_hash) const { 
        return m_ht.count(key, precalculated_hash);
    }
    
    
    
    
    iterator find(const Key& key) { return m_ht.find(key); }
    
    /**
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    iterator find(const Key& key, std::size_t precalculated_hash) { return m_ht.find(key, precalculated_hash); }
    
    const_iterator find(const Key& key) const { return m_ht.find(key); }
    
    /**
     * @copydoc find(const Key& key, std::size_t precalculated_hash)
     */
    const_iterator find(const Key& key, std::size_t precalculated_hash) const { 
        return m_ht.find(key, precalculated_hash);
    }
    
    /**
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr>
    iterator find(const K& key) { return m_ht.find(key); }
    
    /**
     * @copydoc find(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    iterator find(const K& key, std::size_t precalculated_hash) { return m_ht.find(key, precalculated_hash); }
    
    /**
     * @copydoc find(const K& key)
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr>
    const_iterator find(const K& key) const { return m_ht.find(key); }
    
    /**
     * @copydoc find(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    const_iterator find(const K& key, std::size_t precalculated_hash) const { 
        return m_ht.find(key, precalculated_hash); 
    }
    
    
    
    std::pair<iterator, iterator> equal_range(const Key& key) { return m_ht.equal_range(key); }
    
    /**
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    std::pair<iterator, iterator> equal_range(const Key& key, std::size_t precalculated_hash) { 
        return m_ht.equal_range(key, precalculated_hash); 
    }
    
    std::pair<const_iterator, const_iterator> equal_range(const Key& key) const { return m_ht.equal_range(key); }
    
    /**
     * @copydoc equal_range(const Key& key, std::size_t precalculated_hash)
     */
    std::pair<const_iterator, const_iterator> equal_range(const Key& key, std::size_t precalculated_hash) const { 
        return m_ht.equal_range(key, precalculated_hash); 
    }
    
    /**
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr>     
    std::pair<iterator, iterator> equal_range(const K& key) { return m_ht.equal_range(key); }
    
    /**
     * @copydoc equal_range(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    std::pair<iterator, iterator> equal_range(const K& key, std::size_t precalculated_hash) { 
        return m_ht.equal_range(key, precalculated_hash); 
    }
    
    /**
     * @copydoc equal_range(const K& key)
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr>     
    std::pair<const_iterator, const_iterator> equal_range(const K& key) const { return m_ht.equal_range(key); }
    
    /**
     * @copydoc equal_range(const K& key, std::size_t precalculated_hash)
     */    
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    std::pair<const_iterator, const_iterator> equal_range(const K& key, std::size_t precalculated_hash) const { 
        return m_ht.equal_range(key, precalculated_hash); 
    }
    

    /*
     * Bucket interface 
     */
    size_type bucket_count() const { return m_ht.bucket_count(); }
    size_type max_bucket_count() const { return m_ht.max_bucket_count(); }
    
    
    /*
     *  Hash policy 
     */
    float load_factor() const { return m_ht.load_factor(); }
    float max_load_factor() const { return m_ht.max_load_factor(); }
    void max_load_factor(float ml) { m_ht.max_load_factor(ml); }
    
    void rehash(size_type count) { m_ht.rehash(count); }
    void reserve(size_type count) { m_ht.reserve(count); }
    
    
    /*
     * Observers
     */
    hasher hash_function() const { return m_ht.hash_function(); }
    key_equal key_eq() const { return m_ht.key_eq(); }
    
    
    /*
     * Other
     */
    
    /**
     * Convert a const_iterator to an iterator.
     */
    iterator mutable_iterator(const_iterator pos) {
        return m_ht.mutable_iterator(pos);
    }
    
    /**
     * Requires index <= size().
     * 
     * Return an iterator to the element at index. Return end() if index == size().
     */
    iterator nth(size_type index) { return m_ht.nth(index); }
    
    /**
     * @copydoc nth(size_type index)
     */
    const_iterator nth(size_type index) const { return m_ht.nth(index); }
    
    
    /**
     * Return const_reference to the first element. Requires the container to not be empty.
     */
    const_reference front() const { return m_ht.front(); }
    
    /**
     * Return const_reference to the last element. Requires the container to not be empty.
     */
    const_reference back() const { return m_ht.back(); }
    
    
    /**
     * Only available if ValueTypeContainer is a std::vector. Same as calling 'values_container().data()'.
     */ 
    template<class U = values_container_type, typename std::enable_if<tsl::detail_ordered_hash::is_vector<U>::value>::type* = nullptr>    
    const typename values_container_type::value_type* data() const noexcept { return m_ht.data(); }
    
    /**
     * Return the container in which the values are stored. The values are in the same order as the insertion order
     * and are contiguous in the structure, no holes (size() == values_container().size()).
     */        
    const values_container_type& values_container() const noexcept { return m_ht.values_container(); }

    template<class U = values_container_type, typename std::enable_if<tsl::detail_ordered_hash::is_vector<U>::value>::type* = nullptr>    
    size_type capacity() const noexcept { return m_ht.capacity(); }
    
    void shrink_to_fit() { m_ht.shrink_to_fit(); }
    
    
    
    /**
     * Insert the value before pos shifting all the elements on the right of pos (including pos) one position 
     * to the right.
     * 
     * Amortized linear time-complexity in the distance between pos and end().
     */
    std::pair<iterator, bool> insert_at_position(const_iterator pos, const value_type& value) { 
        return m_ht.insert_at_position(pos, value); 
    }
    
    /**
     * @copydoc insert_at_position(const_iterator pos, const value_type& value)
     */
    std::pair<iterator, bool> insert_at_position(const_iterator pos, value_type&& value) { 
        return m_ht.insert_at_position(pos, std::move(value)); 
    }
    
    /**
     * @copydoc insert_at_position(const_iterator pos, const value_type& value)
     * 
     * Same as insert_at_position(pos, value_type(std::forward<Args>(args)...), mainly
     * here for coherence.
     */
    template<class... Args>
    std::pair<iterator, bool> emplace_at_position(const_iterator pos, Args&&... args) {
        return m_ht.emplace_at_position(pos, std::forward<Args>(args)...); 
    }
    
    
    
    void pop_back() { m_ht.pop_back(); }
    
    /**
     * Faster erase operation with an O(1) average complexity but it doesn't preserve the insertion order.
     * 
     * If an erasure occurs, the last element of the map will take the place of the erased element.
     */    
    iterator unordered_erase(iterator pos) { return m_ht.unordered_erase(pos); }
    
    /**
     * @copydoc unordered_erase(iterator pos)
     */    
    iterator unordered_erase(const_iterator pos) { return m_ht.unordered_erase(pos); }
    
    /**
     * @copydoc unordered_erase(iterator pos)
     */    
    size_type unordered_erase(const key_type& key) { return m_ht.unordered_erase(key); }
    
    /**
     * @copydoc unordered_erase(iterator pos)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */    
    size_type unordered_erase(const key_type& key, std::size_t precalculated_hash) { 
        return m_ht.unordered_erase(key, precalculated_hash); 
    }
    
    /**
     * @copydoc unordered_erase(iterator pos)
     * 
     * This overload only participates in the overload resolution if the typedef KeyEqual::is_transparent exists. 
     * If so, K must be hashable and comparable to Key.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type unordered_erase(const K& key) { return m_ht.unordered_erase(key); }
    
    /**
     * @copydoc unordered_erase(const K& key)
     * 
     * Use the hash value 'precalculated_hash' instead of hashing the key. The hash value should be the same
     * as hash_function()(key). Usefull to speed-up the lookup if you already have the hash.
     */
    template<class K, class KE = KeyEqual, typename std::enable_if<has_is_transparent<KE>::value>::type* = nullptr> 
    size_type unordered_erase(const K& key, std::size_t precalculated_hash) { 
        return m_ht.unordered_erase(key, precalculated_hash); 
    }
    
    
    
    friend bool operator==(const ordered_set& lhs, const ordered_set& rhs) { return lhs.m_ht == rhs.m_ht; }
    friend bool operator!=(const ordered_set& lhs, const ordered_set& rhs) { return lhs.m_ht != rhs.m_ht; }
    friend bool operator<(const ordered_set& lhs, const ordered_set& rhs) { return lhs.m_ht < rhs.m_ht; }
    friend bool operator<=(const ordered_set& lhs, const ordered_set& rhs) { return lhs.m_ht <= rhs.m_ht; }
    friend bool operator>(const ordered_set& lhs, const ordered_set& rhs) { return lhs.m_ht > rhs.m_ht; }
    friend bool operator>=(const ordered_set& lhs, const ordered_set& rhs) { return lhs.m_ht >= rhs.m_ht; }
    
    friend void swap(ordered_set& lhs, ordered_set& rhs) { lhs.swap(rhs); }
    
private:
    ht m_ht;    
};

} // end namespace tsl

#endif

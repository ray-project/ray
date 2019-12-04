#ifndef RAY_UTIL_PROCESS_H
#define RAY_UTIL_PROCESS_H

#include <functional>  // std::equal_to, std::hash, std::less
#include <memory>      // std::shared_ptr
#include <utility>     // std::forward

#include <boost/process/args.hpp>
#include <boost/process/child.hpp>

// We only define operators required by the standard library (<, ==, hash).
// We declare but avoid defining the rest so that they're not used by accident.

namespace std {
template <>
struct hash<boost::process::child> : hash<boost::process::pid_t> {
  typedef hash<boost::process::pid_t> base_type;
  typedef boost::process::child argument_type;
  typedef size_t result_type;
  result_type operator()(const argument_type &value) const {
    return this->base_type::operator()(value.id());
  }
};
template <>
struct equal_to<boost::process::child> : equal_to<boost::process::pid_t> {
  typedef equal_to<boost::process::pid_t> base_type;
  typedef boost::process::child argument_type;
  typedef bool result_type;
  result_type operator()(const argument_type &a, const argument_type &b) const {
    return this->base_type::operator()(a.id(), b.id());
  }
};
template <>
struct greater<boost::process::child>;
template <>
struct greater_equal<boost::process::child>;
template <>
struct less<boost::process::child> : less<boost::process::pid_t> {
  typedef less<boost::process::pid_t> base_type;
  typedef boost::process::child argument_type;
  typedef bool result_type;
  result_type operator()(const argument_type &a, const argument_type &b) const {
    return this->base_type::operator()(a.id(), b.id());
  }
};
template <>
struct less_equal<boost::process::child>;
template <>
struct not_equal_to<boost::process::child>;

// Invalid process objects must be distinguished by their addresses.
//   Valid process objects must be distinguished by their IDs.
template <>
struct hash<shared_ptr<boost::process::child>> : hash<boost::process::child> {
  typedef hash<boost::process::child> base_type;
  typedef shared_ptr<boost::process::child> argument_type;
  typedef size_t result_type;
  result_type operator()(const argument_type &value) const {
    // For explanation, see note above.
    return value ? value->valid() ? this->base_type::operator()(*value)
                                  : hash<void const *>()(value.get())
                 : result_type();
  }
};
template <>
struct equal_to<shared_ptr<boost::process::child>> : equal_to<boost::process::child> {
  typedef equal_to<boost::process::child> base_type;
  typedef shared_ptr<boost::process::child> argument_type;
  typedef bool result_type;
  result_type operator()(const argument_type &a, const argument_type &b) const {
    // For explanation, see note above.
    return a ? b ? a->valid()
                       ? b->valid() ? this->base_type::operator()(*a, *b) : false
                       : b->valid() ? false
                                    : std::equal_to<void const *>()(a.get(), b.get())
                 : false
             : !b;
  }
};
template <>
struct not_equal_to<shared_ptr<boost::process::child>>;
template <>
struct greater<shared_ptr<boost::process::child>>;
template <>
struct greater_equal<shared_ptr<boost::process::child>>;
template <>
struct less<shared_ptr<boost::process::child>> : less<boost::process::child> {
  typedef less<boost::process::child> base_type;
  typedef shared_ptr<boost::process::child> argument_type;
  typedef bool result_type;
  result_type operator()(const argument_type &a, const argument_type &b) const {
    // For explanation, see note above.
    return a ? b ? a->valid()
                       ? b->valid() ? this->base_type::operator()(*a, *b) : false
                       : b->valid() ? true : std::less<void const *>()(a.get(), b.get())
                 : false
             : !!b;
  }
};
template <>
struct less_equal<shared_ptr<boost::process::child>>;
}  // namespace std

namespace ray {
typedef boost::process::pid_t pid_t;
typedef boost::process::child Process;
static constexpr boost::process::detail::args_ make_process_args;
}  // namespace ray

#endif

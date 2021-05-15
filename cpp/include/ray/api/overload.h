// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

namespace ray {
namespace internal {

struct cv_none {};
struct cv_const {};
struct cv_volatile {};
struct cv_cv {};
struct ref_none {};
struct ref_rval {};
struct ref_lval {};

namespace detail {

//
// underload for free functions
//

template <typename... Ts>
struct underload_free {
  template <typename R>
  constexpr auto operator()(R (*f)(Ts...)) const {
    return f;
  }
};

//
// underload for member functions (also free functions for convenience)
// unspecialized template accepts any cv qualifier, any reference qualifier
// and therefore cannot resolve an overload that differs only in qualification
//

template <typename... Ts>
struct underload : underload_free<Ts...>,
                   underload<cv_none, Ts...>,
                   underload<cv_const, Ts...>,
                   underload<cv_volatile, Ts...>,
                   underload<cv_cv, Ts...> {
  using underload_free<Ts...>::operator();
  using underload<cv_none, Ts...>::operator();
  using underload<cv_const, Ts...>::operator();
  using underload<cv_volatile, Ts...>::operator();
  using underload<cv_cv, Ts...>::operator();
  static constexpr bool is_overload_v = true;
};

//
// specializations with cv tag
// accept any reference qualifier
// cannot resolve overload that differs only in reference qualification
//

template <typename... Ts>
struct underload<cv_none, Ts...> : underload<cv_none, ref_none, Ts...>,
                                   underload<cv_none, ref_rval, Ts...>,
                                   underload<cv_none, ref_lval, Ts...> {
  using underload<cv_none, ref_none, Ts...>::operator();
  using underload<cv_none, ref_rval, Ts...>::operator();
  using underload<cv_none, ref_lval, Ts...>::operator();
};

template <typename... Ts>
struct underload<cv_const, Ts...> : underload<cv_const, ref_none, Ts...>,
                                    underload<cv_const, ref_rval, Ts...>,
                                    underload<cv_const, ref_lval, Ts...> {
  using underload<cv_const, ref_none, Ts...>::operator();
  using underload<cv_const, ref_rval, Ts...>::operator();
  using underload<cv_const, ref_lval, Ts...>::operator();
};

template <typename... Ts>
struct underload<cv_volatile, Ts...> : underload<cv_volatile, ref_none, Ts...>,
                                       underload<cv_volatile, ref_rval, Ts...>,
                                       underload<cv_volatile, ref_lval, Ts...> {
  using underload<cv_volatile, ref_none, Ts...>::operator();
  using underload<cv_volatile, ref_rval, Ts...>::operator();
  using underload<cv_volatile, ref_lval, Ts...>::operator();
};

template <typename... Ts>
struct underload<cv_cv, Ts...> : underload<cv_cv, ref_none, Ts...>,
                                 underload<cv_cv, ref_rval, Ts...>,
                                 underload<cv_cv, ref_lval, Ts...> {
  using underload<cv_cv, ref_none, Ts...>::operator();
  using underload<cv_cv, ref_rval, Ts...>::operator();
  using underload<cv_cv, ref_lval, Ts...>::operator();
};

//
// specializations with reference tag
// accept any cv qualifier
// cannot resolve overload that differs only in cv qualification
//

template <typename... Ts>
struct underload<ref_none, Ts...> : underload<cv_none, ref_none, Ts...>,
                                    underload<cv_const, ref_none, Ts...>,
                                    underload<cv_volatile, ref_none, Ts...>,
                                    underload<cv_cv, ref_none, Ts...> {
  using underload<cv_none, ref_none, Ts...>::operator();
  using underload<cv_const, ref_none, Ts...>::operator();
  using underload<cv_volatile, ref_none, Ts...>::operator();
  using underload<cv_cv, ref_none, Ts...>::operator();
};

template <typename... Ts>
struct underload<ref_rval, Ts...> : underload<cv_none, ref_rval, Ts...>,
                                    underload<cv_const, ref_rval, Ts...>,
                                    underload<cv_volatile, ref_rval, Ts...>,
                                    underload<cv_cv, ref_rval, Ts...> {
  using underload<cv_none, ref_rval, Ts...>::operator();
  using underload<cv_const, ref_rval, Ts...>::operator();
  using underload<cv_volatile, ref_rval, Ts...>::operator();
  using underload<cv_cv, ref_rval, Ts...>::operator();
};

template <typename... Ts>
struct underload<ref_lval, Ts...> : underload<cv_none, ref_lval, Ts...>,
                                    underload<cv_const, ref_lval, Ts...>,
                                    underload<cv_volatile, ref_lval, Ts...>,
                                    underload<cv_cv, ref_lval, Ts...> {
  using underload<cv_none, ref_lval, Ts...>::operator();
  using underload<cv_const, ref_lval, Ts...>::operator();
  using underload<cv_volatile, ref_lval, Ts...>::operator();
  using underload<cv_cv, ref_lval, Ts...>::operator();
};

//
// specializations with cv tag followed by reference tag
//

#define UNDERLOAD(CV_TAG, REF_TAG, QUALIFIER)                     \
  template <typename... Ts>                                       \
  struct underload<CV_TAG, REF_TAG, Ts...> {                      \
    template <typename R, typename T>                             \
    constexpr auto operator()(R (T::*f)(Ts...) QUALIFIER) const { \
      return f;                                                   \
    }                                                             \
    static constexpr bool is_cv_v = true;                         \
  };
UNDERLOAD(cv_none, ref_none, )
UNDERLOAD(cv_const, ref_none, const)
UNDERLOAD(cv_volatile, ref_none, volatile)
UNDERLOAD(cv_cv, ref_none, const volatile)
UNDERLOAD(cv_none, ref_rval, &&)
UNDERLOAD(cv_const, ref_rval, const &&)
UNDERLOAD(cv_volatile, ref_rval, volatile &&)
UNDERLOAD(cv_cv, ref_rval, const volatile &&)
UNDERLOAD(cv_none, ref_lval, &)
UNDERLOAD(cv_const, ref_lval, const &)
UNDERLOAD(cv_volatile, ref_lval, volatile &)
UNDERLOAD(cv_cv, ref_lval, const volatile &)
#undef UNDERLOAD

//
// specializations with reference tag followed by cv tag
//

#define UNDERLOAD(CV_TAG, REF_TAG) \
  template <typename... Ts>        \
  struct underload<REF_TAG, CV_TAG, Ts...> : underload<CV_TAG, REF_TAG, Ts...> {};
UNDERLOAD(cv_none, ref_none)
UNDERLOAD(cv_const, ref_none)
UNDERLOAD(cv_volatile, ref_none)
UNDERLOAD(cv_cv, ref_none)
UNDERLOAD(cv_none, ref_rval)
UNDERLOAD(cv_const, ref_rval)
UNDERLOAD(cv_volatile, ref_rval)
UNDERLOAD(cv_cv, ref_rval)
UNDERLOAD(cv_none, ref_lval)
UNDERLOAD(cv_const, ref_lval)
UNDERLOAD(cv_volatile, ref_lval)
UNDERLOAD(cv_cv, ref_lval)
#undef UNDERLOAD

}  // namespace detail

template <typename... Ts>
constexpr detail::underload_free<Ts...> underload_free{};

template <typename... Ts>
constexpr detail::underload<Ts...> underload{};

}  // namespace internal
}  // namespace ray

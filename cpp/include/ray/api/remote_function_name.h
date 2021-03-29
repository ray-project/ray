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

#include <array>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include "absl/strings/string_view.h"

namespace ray {
namespace internal {

/******************************************/
#define ADD_VIEW(str) absl::string_view(#str, sizeof(#str) - 1)

#define SEPERATOR ,
#define CON_STR_1(element, ...) ADD_VIEW(element)
#define CON_STR_2(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_1(__VA_ARGS__))
#define CON_STR_3(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_2(__VA_ARGS__))
#define CON_STR_4(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_3(__VA_ARGS__))
#define CON_STR_5(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_4(__VA_ARGS__))
#define CON_STR_6(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_5(__VA_ARGS__))
#define CON_STR_7(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_6(__VA_ARGS__))
#define CON_STR_8(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_7(__VA_ARGS__))
#define CON_STR_9(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_8(__VA_ARGS__))
#define CON_STR_10(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_9(__VA_ARGS__))
#define CON_STR_11(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_10(__VA_ARGS__))
#define CON_STR_12(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_11(__VA_ARGS__))
#define CON_STR_13(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_12(__VA_ARGS__))
#define CON_STR_14(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_13(__VA_ARGS__))
#define CON_STR_15(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_14(__VA_ARGS__))
#define CON_STR_16(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_15(__VA_ARGS__))
#define CON_STR_17(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_16(__VA_ARGS__))
#define CON_STR_18(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_17(__VA_ARGS__))
#define CON_STR_19(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_18(__VA_ARGS__))
#define CON_STR_20(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_19(__VA_ARGS__))
#define CON_STR_21(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_20(__VA_ARGS__))
#define CON_STR_22(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_21(__VA_ARGS__))
#define CON_STR_23(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_22(__VA_ARGS__))
#define CON_STR_24(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_23(__VA_ARGS__))
#define CON_STR_25(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_24(__VA_ARGS__))
#define CON_STR_26(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_25(__VA_ARGS__))
#define CON_STR_27(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_26(__VA_ARGS__))
#define CON_STR_28(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_27(__VA_ARGS__))
#define CON_STR_29(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_28(__VA_ARGS__))
#define CON_STR_30(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_29(__VA_ARGS__))
#define CON_STR_31(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_30(__VA_ARGS__))
#define CON_STR_32(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_31(__VA_ARGS__))
#define CON_STR_33(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_32(__VA_ARGS__))
#define CON_STR_34(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_33(__VA_ARGS__))
#define CON_STR_35(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_34(__VA_ARGS__))
#define CON_STR_36(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_35(__VA_ARGS__))
#define CON_STR_37(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_36(__VA_ARGS__))
#define CON_STR_38(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_37(__VA_ARGS__))
#define CON_STR_39(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_38(__VA_ARGS__))
#define CON_STR_40(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_39(__VA_ARGS__))
#define CON_STR_41(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_40(__VA_ARGS__))
#define CON_STR_42(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_41(__VA_ARGS__))
#define CON_STR_43(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_42(__VA_ARGS__))
#define CON_STR_44(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_43(__VA_ARGS__))
#define CON_STR_45(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_44(__VA_ARGS__))
#define CON_STR_46(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_45(__VA_ARGS__))
#define CON_STR_47(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_46(__VA_ARGS__))
#define CON_STR_48(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_47(__VA_ARGS__))
#define CON_STR_49(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_48(__VA_ARGS__))
#define CON_STR_50(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_49(__VA_ARGS__))
#define CON_STR_51(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_50(__VA_ARGS__))
#define CON_STR_52(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_51(__VA_ARGS__))
#define CON_STR_53(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_52(__VA_ARGS__))
#define CON_STR_54(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_53(__VA_ARGS__))
#define CON_STR_55(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_54(__VA_ARGS__))
#define CON_STR_56(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_55(__VA_ARGS__))
#define CON_STR_57(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_56(__VA_ARGS__))
#define CON_STR_58(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_57(__VA_ARGS__))
#define CON_STR_59(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_58(__VA_ARGS__))
#define CON_STR_60(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_59(__VA_ARGS__))
#define CON_STR_61(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_60(__VA_ARGS__))
#define CON_STR_62(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_61(__VA_ARGS__))
#define CON_STR_63(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_62(__VA_ARGS__))
#define CON_STR_64(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_63(__VA_ARGS__))
#define CON_STR_65(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_64(__VA_ARGS__))
#define CON_STR_66(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_65(__VA_ARGS__))
#define CON_STR_67(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_66(__VA_ARGS__))
#define CON_STR_68(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_67(__VA_ARGS__))
#define CON_STR_69(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_68(__VA_ARGS__))
#define CON_STR_70(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_69(__VA_ARGS__))
#define CON_STR_71(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_70(__VA_ARGS__))
#define CON_STR_72(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_71(__VA_ARGS__))
#define CON_STR_73(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_72(__VA_ARGS__))
#define CON_STR_74(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_73(__VA_ARGS__))
#define CON_STR_75(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_74(__VA_ARGS__))
#define CON_STR_76(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_75(__VA_ARGS__))
#define CON_STR_77(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_76(__VA_ARGS__))
#define CON_STR_78(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_77(__VA_ARGS__))
#define CON_STR_79(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_78(__VA_ARGS__))
#define CON_STR_80(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_79(__VA_ARGS__))
#define CON_STR_81(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_80(__VA_ARGS__))
#define CON_STR_82(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_81(__VA_ARGS__))
#define CON_STR_83(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_82(__VA_ARGS__))
#define CON_STR_84(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_83(__VA_ARGS__))
#define CON_STR_85(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_84(__VA_ARGS__))
#define CON_STR_86(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_85(__VA_ARGS__))
#define CON_STR_87(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_86(__VA_ARGS__))
#define CON_STR_88(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_87(__VA_ARGS__))
#define CON_STR_89(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_88(__VA_ARGS__))
#define CON_STR_90(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_89(__VA_ARGS__))
#define CON_STR_91(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_90(__VA_ARGS__))
#define CON_STR_92(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_91(__VA_ARGS__))
#define CON_STR_93(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_92(__VA_ARGS__))
#define CON_STR_94(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_93(__VA_ARGS__))
#define CON_STR_95(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_94(__VA_ARGS__))
#define CON_STR_96(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_95(__VA_ARGS__))
#define CON_STR_97(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_96(__VA_ARGS__))
#define CON_STR_98(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_97(__VA_ARGS__))
#define CON_STR_99(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_98(__VA_ARGS__))
#define CON_STR_100(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_99(__VA_ARGS__))
#define CON_STR_101(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_100(__VA_ARGS__))
#define CON_STR_102(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_101(__VA_ARGS__))
#define CON_STR_103(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_102(__VA_ARGS__))
#define CON_STR_104(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_103(__VA_ARGS__))
#define CON_STR_105(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_104(__VA_ARGS__))
#define CON_STR_106(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_105(__VA_ARGS__))
#define CON_STR_107(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_106(__VA_ARGS__))
#define CON_STR_108(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_107(__VA_ARGS__))
#define CON_STR_109(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_108(__VA_ARGS__))
#define CON_STR_110(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_109(__VA_ARGS__))
#define CON_STR_111(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_110(__VA_ARGS__))
#define CON_STR_112(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_111(__VA_ARGS__))
#define CON_STR_113(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_112(__VA_ARGS__))
#define CON_STR_114(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_113(__VA_ARGS__))
#define CON_STR_115(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_114(__VA_ARGS__))
#define CON_STR_116(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_115(__VA_ARGS__))
#define CON_STR_117(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_116(__VA_ARGS__))
#define CON_STR_118(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_117(__VA_ARGS__))
#define CON_STR_119(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_118(__VA_ARGS__))
#define CON_STR_120(element, ...) \
  ADD_VIEW(element) SEPERATOR MARCO_EXPAND(CON_STR_119(__VA_ARGS__))
#define RSEQ_N()                                                                         \
  119, 118, 117, 116, 115, 114, 113, 112, 111, 110, 109, 108, 107, 106, 105, 104, 103,   \
      102, 101, 100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84, 83, \
      82, 81, 80, 79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68, 67, 66, 65, 64, 63,    \
      62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43,    \
      42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23,    \
      22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0

#define ARG_N(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16,     \
              _17, _18, _19, _20, _21, _22, _23, _24, _25, _26, _27, _28, _29, _30, _31, \
              _32, _33, _34, _35, _36, _37, _38, _39, _40, _41, _42, _43, _44, _45, _46, \
              _47, _48, _49, _50, _51, _52, _53, _54, _55, _56, _57, _58, _59, _60, _61, \
              _62, _63, _64, _65, _66, _67, _68, _69, _70, _71, _72, _73, _74, _75, _76, \
              _77, _78, _79, _80, _81, _82, _83, _84, _85, _86, _87, _88, _89, _90, _91, \
              _92, _93, _94, _95, _96, _97, _98, _99, _100, _101, _102, _103, _104,      \
              _105, _106, _107, _108, _109, _110, _111, _112, _113, _114, _115, _116,    \
              _117, _118, _119, N, ...)                                                  \
  N

#define MARCO_EXPAND(...) __VA_ARGS__

// note use MACRO_CONCAT like A##_##B direct may cause marco expand error
#define MACRO_CONCAT(A, B) MACRO_CONCAT1(A, B)
#define MACRO_CONCAT1(A, B) A##_##B

#define GET_ARG_COUNT_INNER(...) MARCO_EXPAND(ARG_N(__VA_ARGS__))
#define GET_ARG_COUNT(...) GET_ARG_COUNT_INNER(__VA_ARGS__, RSEQ_N())

}  // namespace internal
}  // namespace ray
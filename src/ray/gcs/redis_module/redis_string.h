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

#ifndef RAY_REDIS_STRING_H_
#define RAY_REDIS_STRING_H_

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "redismodule.h"

/* Format a RedisModuleString.
 *
 * @param ctx The redis module context.
 * @param fmt The format string. This currently supports %S for
 *            RedisModuleString and %s for null terminated C strings.
 * @params ... The parameters to be formated.
 * @return The formatted RedisModuleString, needs to be freed by the caller.
 */
RedisModuleString *RedisString_Format(RedisModuleCtx *ctx, const char *fmt, ...) {
  RedisModuleString *result = RedisModule_CreateString(ctx, "", 0);
  size_t initlen = strlen(fmt);
  size_t l;
  RedisModuleString *redisstr;
  const char *s;
  const char *f = fmt;
  int i;
  va_list ap;

  va_start(ap, fmt);
  f = fmt; /* Next format specifier byte to process. */
  i = initlen;
  while (*f) {
    char next;
    switch (*f) {
    case '%':
      next = *(f + 1);
      f++;
      switch (next) {
      case 'S':
        redisstr = va_arg(ap, RedisModuleString *);
        s = RedisModule_StringPtrLen(redisstr, &l);
        RedisModule_StringAppendBuffer(ctx, result, s, l);
        i += 1;
        break;
      case 's':
        s = va_arg(ap, const char *);
        RedisModule_StringAppendBuffer(ctx, result, s, strlen(s));
        i += 1;
        break;
      case 'b':
        s = va_arg(ap, const char *);
        l = va_arg(ap, size_t);
        RedisModule_StringAppendBuffer(ctx, result, s, l);
        i += 1;
        break;
      default: /* Handle %% and generally %<unknown>. */
        RedisModule_StringAppendBuffer(ctx, result, &next, 1);
        i += 1;
        break;
      }
      break;
    default:
      RedisModule_StringAppendBuffer(ctx, result, f, 1);
      i += 1;
      break;
    }
    f += 1;
  }
  va_end(ap);
  return result;
}

std::string RedisString_ToString(RedisModuleString *string) {
  size_t size;
  const char *data = RedisModule_StringPtrLen(string, &size);
  return std::string(data, size);
}

#endif  // RAY_REDIS_STRING_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

RedisModuleString *RedisString_Format(RedisModuleCtx *ctx,
                                      const char *fmt,
                                      ...) {
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
  return result;
}

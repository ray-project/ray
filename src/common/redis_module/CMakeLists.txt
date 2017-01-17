cmake_minimum_required(VERSION 2.8)

project(ray_redis_module)

if(APPLE)
  set(REDIS_MODULE_CFLAGS -W -Wall -dynamic -fno-common -g -ggdb -std=c99 -O2)
  set(REDIS_MODULE_LDFLAGS "-undefined dynamic_lookup")
else()
  set(REDIS_MODULE_CFLAGS -W -Wall -fno-common -g -ggdb -std=c99 -O2)
  set(REDIS_MODULE_LDFLAGS -shared)
endif()

add_library(ray_redis_module SHARED ray_redis_module.c)

target_compile_options(ray_redis_module PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(ray_redis_module ${REDIS_MODULE_LDFLAGS})

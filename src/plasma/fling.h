// Copyright 2013 Sharvil Nanavati
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// FLING: Exchanging file descriptors over sockets
//
// This is a little library for sending file descriptors over a socket
// between processes. The reason for doing that (as opposed to using
// filenames to share the files) is so (a) no files remain in the
// filesystem after all the processes terminate, (b) to make sure that
// there are no name collisions and (c) to be able to control who has
// access to the data.
//
// Most of the code is from https://github.com/sharvil/flingfd

#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

// This is neccessary for Mac OS X, see http://www.apuebook.com/faqs2e.html
// (10).
#if !defined(CMSG_SPACE) && !defined(CMSG_LEN)
#define CMSG_SPACE(len) (__DARWIN_ALIGN32(sizeof(struct cmsghdr)) + __DARWIN_ALIGN32(len))
#define CMSG_LEN(len) (__DARWIN_ALIGN32(sizeof(struct cmsghdr)) + (len))
#endif

void init_msg(struct msghdr* msg, struct iovec* iov, char* buf, size_t buf_len);

// Send a file descriptor over a unix domain socket.
//
// @param conn Unix domain socket to send the file descriptor over.
// @param fd File descriptor to send over.
// @return Status code which is < 0 on failure.
int send_fd(int conn, int fd);

// Receive a file descriptor over a unix domain socket.
//
// @param conn Unix domain socket to receive the file descriptor from.
// @return File descriptor or a value < 0 on failure.
int recv_fd(int conn);

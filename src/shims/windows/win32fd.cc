#undef WIN32_REPLACE_FD_APIS
#define WIN32_REPLACE_FD_APIS 0  // make sure we don't replace the APIs for our own calls
#include "win32fd.h"

#include <errno.h>
#include <fcntl.h>
#include <io.h>

#ifndef _WINSOCKAPI_
#include <WinSock2.h>
#endif

#ifndef __cplusplus
extern "C" {
#endif

enum { fh_prefer_fallback = 1 };
static const char fh_magic_prefix[] = "Win32FD";
static const size_t fh_magic_size = sizeof(fh_magic_prefix) + sizeof(void *);

static void fh_get_magic(unsigned char magic[fh_magic_size]) {
  memcpy(magic, fh_magic_prefix, sizeof(fh_magic_prefix));
  const void *const suffix = GetModuleHandle(NULL);
  memcpy(magic + sizeof(fh_magic_prefix), &suffix, sizeof(suffix));
}

struct fh_payload_t {
  intptr_t handle;
  unsigned char magic[fh_magic_size];
};

static int _fh_close(intptr_t handle) {
  int result = closesocket(handle);
  if (result != 0) {
    int error = WSAGetLastError();
    if (error == WSANOTINITIALISED || error == WSAENOTSOCK) {
      if (CloseHandle(reinterpret_cast<HANDLE>(handle))) {
        result = 0;
      } else {
        _set_errno(EBADF);
      }
    } else {
      _set_errno(EINVAL);
    }
  }
  return result;
}

static int _fh_is_socket(intptr_t handle) {
  int result = 1;
  SOCKADDR_STORAGE addr;
  int addrlen = sizeof(addr);
  if (getsockname(handle, reinterpret_cast<sockaddr *>(&addr), &addrlen) != 0) {
    int error = WSAGetLastError();
    if (error == WSANOTINITIALISED || error == WSAENOTSOCK) {
      result = 0;
    }
  }
  return result;
}

int fh_accept(int sockpfd, struct sockaddr *name, socklen_t *namelen) {
  int result = -1;
  intptr_t handle = fh_get(sockpfd);
  intptr_t accepted = WSAAccept(handle, name, namelen, NULL, NULL);
  if (accepted != -1) {
    result = fh_open(accepted, -1);
    if (result == -1) {
      closesocket(accepted);
      accepted = -1;
    }
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_bind(int sockpfd, const struct sockaddr *name, socklen_t namelen) {
  intptr_t handle = fh_get(sockpfd);
  int result = bind(handle, name, namelen);
  if (result == 0) {
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_close(int pfd) {
  intptr_t handle = fh_get(pfd);
  int result = _close(pfd);
  if (result == 0 && handle != -1) {
    result = _fh_close(handle);
  }
  return result;
}

int fh_connect(int sockpfd, const struct sockaddr *name, socklen_t namelen) {
  intptr_t handle = fh_get(sockpfd);
  int result = WSAConnect(handle, name, namelen, NULL, NULL, NULL, NULL);
  if (result == 0) {
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    int error = WSAGetLastError();
    switch (error) {
    case WSAEINVAL:
    case WSAEWOULDBLOCK:
    case WSA_IO_PENDING:
      error = WSAEINPROGRESS;  // for Redis
      break;
    }
    _set_errno(error);
  }
  return result;
}

int fh_dup(int pfd) {
  int result = -1;
  intptr_t handle = fh_get(pfd);
  if (fh_prefer_fallback && handle == -1) {
    // Not our FD; fall back to default
    result = _dup(pfd);
  } else {
    intptr_t duped = -1;
    WSAPROTOCOL_INFO pi;
    if (WSADuplicateSocket(handle, GetCurrentProcessId(), &pi) == 0) {
      duped = WSASocket(pi.iAddressFamily, pi.iSocketType, pi.iProtocol, &pi, 0, 0);
      if (duped == -1) {
        _set_errno(EINVAL);
      }
    } else {
      int error = WSAGetLastError();
      if (error == WSANOTINITIALISED || error == WSAENOTSOCK) {
        if (!DuplicateHandle(GetCurrentProcess(), reinterpret_cast<HANDLE>(handle),
                             GetCurrentProcess(), reinterpret_cast<HANDLE *>(&duped), 0,
                             FALSE, DUPLICATE_SAME_ACCESS)) {
          duped = -1;
          _set_errno(EBADF);
        }
      } else {
        _set_errno(EBADF);
      }
    }
    if (duped != -1) {
      result = fh_open(duped, -1);
      if (result == -1) {
        _fh_close(duped);
      }
    }
  }
  return result;
}

intptr_t fh_get(int pfd) {
  fh_payload_t payload = {-1};
  HANDLE pipe = reinterpret_cast<HANDLE>(_get_osfhandle(pfd));
  DWORD size = sizeof(payload), nbytes, avail, left;
  if (pipe != INVALID_HANDLE_VALUE) {
    if (GetFileType(pipe) != FILE_TYPE_PIPE) {
      _set_errno(EBADF);  // if this triggers, you passed an invalid or incompatible FD
    } else if (PeekNamedPipe(pipe, &payload, size, &nbytes, &avail, &left)) {
      if (avail != size) {
        payload.handle = -1;
        _set_errno(EIO);  // if this triggers, you accidentally wrote to the FD directly
      } else {
        unsigned char expected[fh_magic_size];
        fh_get_magic(expected);
        if (memcmp(payload.magic, expected, sizeof(expected)) != 0) {
          payload.handle = -1;
          _set_errno(EBADF);  // if this triggers, this isn't a recognized FD
        }
      }
    } else {
      _set_errno(EIO);  // if this triggers, you accidentally read from the FD directly
    }
  } else {
    _set_errno(EBADF);
  }
  return payload.handle;
}

int fh_getpeername(int sockpfd, struct sockaddr *name, socklen_t *namelen) {
  intptr_t handle = fh_get(sockpfd);
  int result = getpeername(handle, name, namelen);
  if (result == 0) {
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_getsockname(int sockpfd, struct sockaddr *name, socklen_t *namelen) {
  intptr_t handle = fh_get(sockpfd);
  int result = getsockname(handle, name, namelen);
  if (result == 0) {
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_getsockopt(int sockpfd, int level, int name, void *val, socklen_t *len) {
  intptr_t handle = fh_get(sockpfd);
  int result = getsockopt(handle, level, name, static_cast<char *>(val), len);
  if (result == 0) {
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_ioctlsocket(int sockpfd, long cmd, unsigned long *argp) {
  intptr_t handle = fh_get(sockpfd);
  int result = ioctlsocket(handle, cmd, argp);
  if (result == 0) {
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_listen(int sockpfd, int backlog) {
  intptr_t handle = fh_get(sockpfd);
  int result = listen(handle, backlog);
  if (result == 0) {
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_open(intptr_t handle, int mode) {
  int pfd = -1;
  HANDLE r, w;
  if (mode == -1) {
    mode = _O_RDWR;
  }
  if (!(mode & _O_TEXT)) {
    mode |= _O_BINARY;
  }
  if (handle == -1 || handle == 0) {
    _set_errno(EBADF);
  } else if (fh_prefer_fallback &&
             GetFileType(reinterpret_cast<HANDLE>(handle)) != FILE_TYPE_UNKNOWN &&
             !_fh_is_socket(handle)) {
    // Already compatible with CRT FDs, so just fall back
    pfd = _open_osfhandle(handle, mode);
  } else {
    fh_payload_t payload = {handle};
    fh_get_magic(payload.magic);
    DWORD size = sizeof(payload), nbytes;
    if (CreatePipe(&r, &w, NULL, size)) {
      if (WriteFile(w, &payload, size, &nbytes, NULL) && size == nbytes) {
        pfd = _open_osfhandle(reinterpret_cast<intptr_t>(r), mode);
        if (pfd == -1) {
          CloseHandle(r);
        }
      } else {
        _set_errno(EIO);
      }
      CloseHandle(w);
    } else {
      _set_errno(EMFILE);
    }
  }
  return pfd;
}

int fh_poll(struct pollfd *fds, nfds_t nfds, int timeout) {
  struct fd_sets_t {
    fd_set rfds, wfds, efds;
  } fdsets;
  int maxfd = -1;
  struct fd_sets_t *pfdsets =
      nfds <= FD_SETSIZE ? &fdsets
                         : static_cast<struct fd_sets_t *>(operator new(
                               (offsetof(fd_set, fd_array) + nfds * sizeof(SOCKET)) *
                               (sizeof(struct fd_sets_t) / sizeof(fd_set))));
  FD_ZERO(&pfdsets->rfds);
  FD_ZERO(&pfdsets->wfds);
  FD_ZERO(&pfdsets->efds);
  for (nfds_t i = 0; i < nfds; ++i) {
    if (fds[i].events & POLLIN) {
      FD_SET(fds[i].fd, &pfdsets->rfds);
    }
    if (fds[i].events & POLLOUT) {
      FD_SET(fds[i].fd, &pfdsets->wfds);
    }
    if (fds[i].events & POLLERR) {
      FD_SET(fds[i].fd, &pfdsets->efds);
    }
    if (maxfd < fds[i].fd) {
      maxfd = static_cast<int>(fds[i].fd);
    }
  }
  int msec = timeout;
  struct timeval tv = {msec >= 0 ? msec / 1000 : 0, msec >= 0 ? (msec % 1000) * 1000 : 0};
  int result = fh_select(maxfd + 1, &pfdsets->rfds, &pfdsets->wfds, &pfdsets->efds,
                         msec >= 0 ? &tv : NULL);
  if (result >= 0) {
    result = 0;
    for (nfds_t i = 0; i < nfds; ++i) {
      fds[i].revents = 0;
      if (FD_ISSET(fds[i].fd, &pfdsets->rfds)) {
        fds[i].revents |= POLLIN;
      }
      if (FD_ISSET(fds[i].fd, &pfdsets->wfds)) {
        fds[i].revents |= POLLOUT;
      }
      if (FD_ISSET(fds[i].fd, &pfdsets->efds)) {
        fds[i].revents |= POLLERR;
      }
      if (fds[i].revents) {
        ++result;
      }
    }
  }
  if (pfdsets != &fdsets) {
    operator delete(pfdsets);
  }
  return result;
}

ssize_t fh_read(int pfd, void *buffer, size_t size) {
  ssize_t result = -1;
  intptr_t handle = fh_get(pfd);
  if (size >= INT_MAX) {
    size = INT_MAX;
  }
  if (fh_prefer_fallback && handle == -1) {
    // Not our FD; fall back to default
    result = _read(pfd, buffer, static_cast<unsigned int>(size));
  } else {
    WSABUF buf = {static_cast<unsigned long>(size), static_cast<char *>(buffer)};
    DWORD nbytes;
    DWORD flags = 0;
    if (WSARecv(handle, &buf, 1, &nbytes, &flags, NULL, NULL) == 0) {
      result = static_cast<ssize_t>(nbytes);
    } else {
      int error = WSAGetLastError();
      if (error == WSANOTINITIALISED || error == WSAENOTSOCK) {
        if (ReadFile(reinterpret_cast<HANDLE>(handle), buffer, static_cast<DWORD>(size),
                     &nbytes, NULL)) {
          result = static_cast<ssize_t>(nbytes);
        } else {
          _set_errno(EINVAL);
        }
      } else {
        if (error == WSAEWOULDBLOCK) {
          error = EAGAIN;  // for Redis
        }
        _set_errno(error);
      }
    }
  }
  return result;
}

ssize_t fh_recv(int sockpfd, void *buffer, size_t size, int flags) {
  ssize_t result = -1;
  if (size >= INT_MAX) {
    size = INT_MAX;
  }
  intptr_t handle = fh_get(sockpfd);
  WSABUF buf = {static_cast<unsigned long>(size), static_cast<char *>(buffer)};
  DWORD nbytes;
  DWORD dwflags = static_cast<DWORD>(flags);
  if (WSARecv(handle, &buf, 1, &nbytes, &dwflags, NULL, NULL) == 0) {
    result = static_cast<ssize_t>(nbytes);
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

intptr_t fh_release(int pfd) {
  intptr_t handle = fh_get(pfd);
  if (handle != -1) {
    if (_close(pfd) != 0) {
      handle = -1;
    }
  }
  return handle;
}

int fh_select(int nfds, fd_set *readfds, fd_set *writefds, fd_set *errorfds,
              struct timeval *timeout) {
  int result = 0;
  fd_set rfds, wfds, efds;
  struct {
    fd_set *src, *dst;
  } translations[] = {
      {readfds, &rfds},
      {writefds, &wfds},
      {errorfds, &efds},
  };
  for (size_t i = 0; i < sizeof(translations) / sizeof(*translations); ++i) {
    fd_set *src = translations[i].src, *dst = translations[i].dst;
    if (src) {
      for (size_t j = 0; j < src->fd_count; ++j) {
        int sockpfd = static_cast<int>(src->fd_array[j]);
        intptr_t handle = fh_get(sockpfd);
        if (handle == -1) {
          result = -1;
          _set_errno(EBADF);
        }
        dst->fd_array[j] = handle;
      }
      dst->fd_count = src->fd_count;
    }
  }
  if (result != -1) {
    result = select(nfds, readfds ? &rfds : NULL, writefds ? &wfds : NULL,
                    errorfds ? &efds : NULL, timeout);
    if (result != -1) {
      _set_errno(WSAGetLastError());
    }
  }
  if (result >= 0) {
    for (size_t i = 0; i < sizeof(translations) / sizeof(*translations); ++i) {
      fd_set *src = translations[i].src, *dst = translations[i].dst;
      if (src) {
        fd_set out;
        FD_ZERO(&out);
        for (size_t j = 0; j < src->fd_count; ++j) {
          int sockpfd = static_cast<int>(src->fd_array[j]);
          intptr_t handle = fh_get(sockpfd);
          if (FD_ISSET(handle, dst)) {
            FD_SET(sockpfd, &out);
          }
        }
        memcpy(src->fd_array, out.fd_array, out.fd_count * sizeof(*out.fd_array));
        src->fd_count = out.fd_count;
      }
    }
  }
  return result;
}

ssize_t fh_send(int sockpfd, const void *buffer, size_t size, int flags) {
  ssize_t result = -1;
  if (size >= INT_MAX) {
    size = INT_MAX;
  }
  intptr_t handle = fh_get(sockpfd);
  WSABUF buf = {static_cast<unsigned long>(size),
                static_cast<char *>(const_cast<void *>(buffer))};
  DWORD nbytes;
  if (WSASend(handle, &buf, 1, &nbytes, flags, NULL, NULL) == 0) {
    result = static_cast<ssize_t>(nbytes);
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_setsockopt(int sockpfd, int level, int name, const void *val, socklen_t len) {
  intptr_t handle = fh_get(sockpfd);
  int result = setsockopt(handle, level, name, static_cast<const char *>(val), len);
  if (result == 0) {
  } else if (handle == -1) {
    _set_errno(EBADF);
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

int fh_socket(int domain, int type, int protocol) {
  int result = -1;
  DWORD flags = WSA_FLAG_OVERLAPPED;  // because socket() is overlapped by default
  intptr_t handle = WSASocket(domain, type, protocol, NULL, 0, flags);
  if (handle != -1) {
    result = fh_open(handle, -1);
    if (result == -1) {
      closesocket(handle);
      handle = -1;
    }
  } else {
    _set_errno(WSAGetLastError());
  }
  return result;
}

ssize_t fh_write(int pfd, const void *buffer, size_t size) {
  ssize_t result = -1;
  ssize_t handle = fh_get(pfd);
  if (size >= INT_MAX) {
    size = INT_MAX;
  }
  if (fh_prefer_fallback && handle == -1) {
    // Not our FD; fall back to default
    result = _write(pfd, buffer, static_cast<unsigned int>(size));
  } else {
    WSABUF buf = {static_cast<unsigned long>(size),
                  static_cast<char *>(const_cast<void *>(buffer))};
    DWORD nbytes;
    DWORD flags = 0;
    if (WSASend(handle, &buf, 1, &nbytes, flags, NULL, NULL) == 0) {
      result = static_cast<ssize_t>(nbytes);
    } else {
      int error = WSAGetLastError();
      if (error == WSANOTINITIALISED || error == WSAENOTSOCK) {
        if (WriteFile(reinterpret_cast<HANDLE>(handle), buffer, static_cast<DWORD>(size),
                      &nbytes, NULL)) {
          result = static_cast<ssize_t>(nbytes);
        } else {
          _set_errno(EINVAL);
        }
      } else {
        _set_errno(WSAGetLastError());
      }
    }
  }
  return result;
}

#ifndef __cplusplus
}
#endif

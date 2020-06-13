#include "ray/util/util.h"

#include <stdio.h>
#include <stdlib.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <process.h>
#include <signal.h>
#include <tchar.h>
#else
#include <sys/un.h>
#endif

#include <algorithm>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/generic/stream_protocol.hpp>
#ifndef _WIN32
#include <boost/asio/local/stream_protocol.hpp>
#endif
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#ifdef _WIN32
#include <boost/asio/windows/object_handle.hpp>
#include <boost/asio/windows/stream_handle.hpp>
#endif
#include <boost/asio/write.hpp>
#include <sstream>
#include <string>
#include <vector>

#include "ray/util/filesystem.h"
#include "ray/util/logging.h"

/// Uses sscanf() to read a token matching from the string, advancing the iterator.
/// \param c_str A string iterator that is dereferenceable. (i.e.: c_str < string::end())
/// \param format The pattern. It must not produce any output. (e.g., use %*d, not %d.)
/// \return The scanned prefix of the string, if any.
static std::string ScanToken(std::string::const_iterator &c_str, std::string format) {
  int i = 0;
  std::string result;
  format += "%n";
  if (static_cast<size_t>(sscanf(&*c_str, format.c_str(), &i)) <= 1) {
    result.insert(result.end(), c_str, c_str + i);
    c_str += i;
  }
  return result;
}

std::string EndpointToUrl(
    const boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol> &ep,
    bool include_scheme) {
  std::string result, scheme;
  switch (ep.protocol().family()) {
  case AF_INET: {
    scheme = "tcp://";
    boost::asio::ip::tcp::endpoint e(boost::asio::ip::tcp::v4(), 0);
    RAY_CHECK(e.size() == ep.size());
    const sockaddr *src = ep.data();
    sockaddr *dst = e.data();
    *reinterpret_cast<sockaddr_in *>(dst) = *reinterpret_cast<const sockaddr_in *>(src);
    std::ostringstream ss;
    ss << e;
    result = ss.str();
    break;
  }
  case AF_INET6: {
    scheme = "tcp://";
    boost::asio::ip::tcp::endpoint e(boost::asio::ip::tcp::v6(), 0);
    RAY_CHECK(e.size() == ep.size());
    const sockaddr *src = ep.data();
    sockaddr *dst = e.data();
    *reinterpret_cast<sockaddr_in6 *>(dst) = *reinterpret_cast<const sockaddr_in6 *>(src);
    std::ostringstream ss;
    ss << e;
    result = ss.str();
    break;
  }
#ifdef BOOST_ASIO_HAS_LOCAL_SOCKETS
  case AF_UNIX:
    scheme = "unix://";
    result.append(reinterpret_cast<const struct sockaddr_un *>(ep.data())->sun_path,
                  ep.size() - offsetof(sockaddr_un, sun_path));
    break;
#endif
  default:
    RAY_LOG(FATAL) << "unsupported protocol family: " << ep.protocol().family();
    break;
  }
  if (include_scheme) {
    result.insert(0, scheme);
  }
  return result;
}

boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol>
ParseUrlEndpoint(const std::string &endpoint, int default_port) {
  // Syntax reference: https://en.wikipedia.org/wiki/URL#Syntax
  // Note that we're a bit more flexible, to allow parsing "127.0.0.1" as a URL.
  boost::asio::generic::stream_protocol::endpoint result;
  std::string address = endpoint, scheme;
  if (address.find("unix://") == 0) {
    scheme = "unix://";
    address.erase(0, scheme.size());
  } else if (address.size() > 0 && ray::IsDirSep(address[0])) {
    scheme = "unix://";
  } else if (address.find("tcp://") == 0) {
    scheme = "tcp://";
    address.erase(0, scheme.size());
  } else {
    scheme = "tcp://";
  }
  if (scheme == "unix://") {
#ifdef BOOST_ASIO_HAS_LOCAL_SOCKETS
    size_t maxlen = sizeof(sockaddr_un().sun_path) / sizeof(*sockaddr_un().sun_path) - 1;
    RAY_CHECK(address.size() <= maxlen)
        << "AF_UNIX path length cannot exceed " << maxlen << " bytes: " << address;
    result = boost::asio::local::stream_protocol::endpoint(address);
#else
    RAY_LOG(FATAL) << "UNIX-domain socket endpoints are not supported: " << endpoint;
#endif
  } else if (scheme == "tcp://") {
    std::string::const_iterator i = address.begin();
    std::string host = ScanToken(i, "[%*[^][/]]");
    host = host.empty() ? ScanToken(i, "%*[^/:]") : host.substr(1, host.size() - 2);
    std::string port_str = ScanToken(i, ":%*d");
    int port = port_str.empty() ? default_port : std::stoi(port_str.substr(1));
    result = boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address(host), port);
  } else {
    RAY_LOG(FATAL) << "Unable to parse socket endpoint: " << endpoint;
  }
  return result;
}

/// Rules:
/// 1. Adjacent tokens are concatenated, so "a"b'c' is just abc
/// 2. Outside quotes: backslashes make the next character literal; space & tab delimit
/// 3. Inside "...": backslashes escape a following " and \ and otherwise stay literal
/// 4. Inside '...': no escaping occurs
/// 5. [&|;<>`#()!] etc. are literal, but should be in '...' to avoid confusion.
/// Note: POSIX shells can perform additional processing (like piping) not reflected here.
/// Refer to the unit tests for examples.
/// To compare against the platform's behavior, try a command like the following:
/// $ python3 -c "import sys; [print(a) for a in sys.argv[1:]]" \x\\y "\x\\y" '\x\\y'
/// Python analog: shlex.split(s)
static std::vector<std::string> ParsePosixCommandLine(const std::string &s) {
  RAY_CHECK(s.find('\0') >= s.size()) << "Invalid null character in command line";
  const char space = ' ', tab = '\t', backslash = '\\', squote = '\'', dquote = '\"';
  char surroundings = space;
  bool escaping = false, arg_started = false;
  std::vector<std::string> result;
  std::string arg;
  for (char ch : s) {
    bool is_delimeter = false;
    if (escaping) {
      if (surroundings == dquote && (ch == backslash || ch == dquote)) {
        arg.pop_back();  // remove backslash because it precedes \ or " in double-quotes
      }
      arg += ch;
      escaping = false;
    } else if (surroundings == dquote || surroundings == squote) {  // inside quotes
      if (ch == surroundings) {
        surroundings = space;  // leaving quotes
      } else {
        arg += ch;
        escaping = surroundings == dquote && ch == backslash;  // backslash in "..."
      }
    } else {  // outside quotes
      if (ch == space || ch == tab) {
        is_delimeter = true;
        if (arg_started) {  // we just finished an argument
          result.push_back(arg);
        }
        arg.clear();
      } else if (ch == dquote || ch == squote) {
        surroundings = ch;  // entering quotes
      } else if (ch == backslash) {
        escaping = true;
      } else {
        arg += ch;
      }
    }
    arg_started = !is_delimeter;
  }
  if (arg_started) {
    result.push_back(arg);
  }
  return result;
}

/// Rules:
/// 1. Adjacent tokens are concatenated, so "a"b"c" is just abc
/// 2. Backslashes escape when eventually followed by " but stay literal otherwise
/// 3. Outside "...": space and tab are delimiters
/// 4. [&|:<>^#()] etc. are literal, but should be in "..." to avoid confusion.
/// Note: Windows tools have additional processing & quirks not reflected here.
/// Refer to the unit tests for examples.
/// To compare against the platform's behavior, try a command like the following:
/// > python3 -c "import sys; [print(a) for a in sys.argv[1:]]" \x\\y "\x\\y"
/// Python analog: None (would be shlex.split(s, posix=False), but it doesn't unquote)
static std::vector<std::string> ParseWindowsCommandLine(const std::string &s) {
  RAY_CHECK(s.find('\0') >= s.size()) << "Invalid null character in command line";
  std::vector<std::string> result;
  std::string arg, c_str = s + '\0';
  std::string::const_iterator i = c_str.begin(), j = c_str.end() - 1;
  for (bool stop = false, in_dquotes = false; !stop;) {
    if (!in_dquotes && (i >= j || ScanToken(i, "%*[ \t]").size())) {
      result.push_back(arg);
      arg.clear();
    }
    stop |= i >= j && !in_dquotes;
    arg += ScanToken(i, in_dquotes ? "%*[^\\\"]" : "%*[^\\\" \t]");
    std::string possible_escape = ScanToken(i, "%*[\\]");
    bool escaping = possible_escape.size() % 2 != 0;
    if (*i == '\"') {
      possible_escape.erase(possible_escape.size() / 2);
      possible_escape.append(escaping ? 1 : 0, *i);
      in_dquotes ^= !escaping;
      ++i;
    }
    arg += possible_escape;
  }
  return result;
}

std::vector<std::string> ParseCommandLine(const std::string &s, CommandLineSyntax kind) {
  if (kind == CommandLineSyntax::System) {
#ifdef _WIN32
    kind = CommandLineSyntax::Windows;
#else
    kind = CommandLineSyntax::POSIX;
#endif
  }
  std::vector<std::string> result;
  switch (kind) {
  case CommandLineSyntax::POSIX:
    result = ParsePosixCommandLine(s);
    break;
  case CommandLineSyntax::Windows:
    result = ParseWindowsCommandLine(s);
    break;
  default:
    RAY_LOG(FATAL) << "invalid command line syntax";
    break;
  }
  return result;
}

/// Python analog: shlex.join(args)
std::string CreatePosixCommandLine(const std::vector<std::string> &args) {
  std::string result;
  const std::string safe_chars("%*[-A-Za-z0-9%_=+]");
  const char single_quote = '\'';
  for (size_t a = 0; a != args.size(); ++a) {
    std::string arg = args[a], arg_with_null = arg + '\0';
    std::string::const_iterator i = arg_with_null.begin();
    if (ScanToken(i, safe_chars) != arg) {
      // Prefer single-quotes. Double-quotes have unpredictable behavior, e.g. for "\!".
      std::string quoted;
      quoted += single_quote;
      for (char ch : arg) {
        if (ch == single_quote) {
          quoted += single_quote;
          quoted += '\\';
        }
        quoted += ch;
        if (ch == single_quote) {
          quoted += single_quote;
        }
      }
      quoted += single_quote;
      arg = quoted;
    }
    if (a > 0) {
      result += ' ';
    }
    result += arg;
  }
  return result;
}

// Python analog: subprocess.list2cmdline(args)
static std::string CreateWindowsCommandLine(const std::vector<std::string> &args) {
  std::string result;
  const std::string safe_chars("%*[-A-Za-z0-9%_=+]");
  const char double_quote = '\"';
  for (size_t a = 0; a != args.size(); ++a) {
    std::string arg = args[a], arg_with_null = arg + '\0';
    std::string::const_iterator i = arg_with_null.begin();
    if (ScanToken(i, safe_chars) != arg) {
      // Escape only backslashes that precede double-quotes
      std::string quoted;
      quoted += double_quote;
      size_t backslashes = 0;
      for (char ch : arg) {
        if (ch == double_quote) {
          quoted.append(backslashes, '\\');
          quoted += '\\';
        }
        quoted += ch;
        backslashes = ch == '\\' ? backslashes + 1 : 0;
      }
      quoted.append(backslashes, '\\');
      quoted += double_quote;
      arg = quoted;
    }
    if (a > 0) {
      result += ' ';
    }
    result += arg;
  }
  return result;
}

std::string CreateCommandLine(const std::vector<std::string> &args,
                              CommandLineSyntax kind) {
  if (kind == CommandLineSyntax::System) {
#ifdef _WIN32
    kind = CommandLineSyntax::Windows;
#else
    kind = CommandLineSyntax::POSIX;
#endif
  }
  std::string result;
  switch (kind) {
  case CommandLineSyntax::POSIX:
    result = CreatePosixCommandLine(args);
    break;
  case CommandLineSyntax::Windows:
    result = CreateWindowsCommandLine(args);
    break;
  default:
    RAY_LOG(FATAL) << "invalid command line syntax";
    break;
  }
  return result;
}

#ifdef _WIN32
static BOOL CreatePipeEx(HANDLE (&handles)[2], DWORD rflags, DWORD wflags, DWORD rbufsize,
                         DWORD wbufsize, BOOL inheritable) {
  static LONG pipe_id = 0;
  TCHAR path[128];
  _stprintf(path, _T("\\\\.\\Pipe\\AnonymousPipe.%d.%d"),
            static_cast<int>(GetCurrentProcessId()),
            static_cast<int>(InterlockedIncrement(&pipe_id)));
  SECURITY_ATTRIBUTES sa = {sizeof(sa), NULL, inheritable};
  DWORD openmode = PIPE_ACCESS_INBOUND | FILE_FLAG_FIRST_PIPE_INSTANCE | rflags,
        pipemode = PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT;
  DWORD count = 1;
  HANDLE server, client = INVALID_HANDLE_VALUE;
  server = CreateNamedPipe(path, openmode, pipemode, count, rbufsize, wbufsize,
                           120 * 1000, &sa);
  if (server != INVALID_HANDLE_VALUE) {
    DWORD filemode = FILE_ATTRIBUTE_NORMAL | wflags;
    client = CreateFile(path, GENERIC_WRITE, 0, &sa, OPEN_EXISTING, filemode, NULL);
  }
  BOOL success = server != INVALID_HANDLE_VALUE && client != INVALID_HANDLE_VALUE;
  if (success) {
    handles[0] = server;
    handles[1] = client;
  } else {
    if (server != INVALID_HANDLE_VALUE) {
      CloseHandle(server);
    }
    if (client != INVALID_HANDLE_VALUE) {
      CloseHandle(client);
    }
  }
  return success;
}
#endif

void AwaitPipeClose(boost::asio::io_service &io_service, int rfd,
                    std::function<void(const boost::system::error_code &, int)> callback,
                    long poll_msec) {
#ifdef _WIN32
  RAY_CHECK(GetFileType(reinterpret_cast<HANDLE>(_get_osfhandle(rfd))) == FILE_TYPE_PIPE);
  int backup_rfd = _dup(rfd);
  RAY_CHECK(backup_rfd != -1) << "Error duplicating pipe FD: " << rfd;
  // TODO(mehrdadn): Check if we can assume stdin is empty and simplify as appropriate
  HANDLE handle = INVALID_HANDLE_VALUE;
  RAY_CHECK(
      DuplicateHandle(GetCurrentProcess(), reinterpret_cast<HANDLE>(_get_osfhandle(rfd)),
                      GetCurrentProcess(), &handle, 0, FALSE, DUPLICATE_SAME_ACCESS))
      << "Error cloning pipe: " << rfd;
  auto stream = std::make_shared<boost::asio::windows::object_handle>(io_service, handle);
  HANDLE pipe[2] = {INVALID_HANDLE_VALUE, INVALID_HANDLE_VALUE};
  unsigned int bufsize = 1 << 13;
  // Create a pipe with synchronous read (like stdin), but asynchronous write
  RAY_CHECK(CreatePipeEx(pipe, 0, FILE_FLAG_OVERLAPPED, bufsize, bufsize, FALSE))
      << "Error creating pipe: " << GetLastError();
  int rpipefd = _open_osfhandle(reinterpret_cast<intptr_t>(pipe[0]), _O_BINARY);
  RAY_CHECK(rpipefd != -1) << "Error opening pipe as FD: " << pipe[0];
  auto wpipe = std::make_shared<boost::asio::windows::stream_handle>(io_service, pipe[1]);
  boost::asio::post(io_service, [=, &io_service]() {
    using namespace boost::system;
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service);
    auto do_read = std::make_shared<std::function<void(const error_code &)>>();
    *do_read = [=, &io_service](const error_code &read_error) mutable {
      RAY_CHECK(!read_error);
      DWORD pending;
      if (PeekNamedPipe(stream->native_handle(), NULL, 0, NULL, &pending, NULL)) {
        if (pending) {
          DWORD size;
          void *data = operator new(pending);
          RAY_CHECK(ReadFile(stream->native_handle(), data, pending, &size, NULL));
          std::function<void(const error_code &, size_t)> do_write =
              [=](const error_code &write_error, size_t) mutable {
                RAY_CHECK(!write_error) << "Error writing to pipe: " << write_error;
                operator delete(data);
                stream->async_wait(*do_read);
              };
          boost::asio::async_write(*wpipe, boost::asio::buffer(data, size), do_write);
          RAY_CHECK(!read_error) << "Error reading from pipe: " << read_error;
        } else {
          timer->expires_from_now(boost::posix_time::milliseconds(poll_msec));
          timer->async_wait(*do_read);
        }
      } else {
        RAY_CHECK(GetLastError() == ERROR_BROKEN_PIPE);
        boost::asio::post(io_service, [=, &io_service]() {
          // Restore file descriptor
          RAY_CHECK(_dup2(backup_rfd, rfd) != -1) << "Failed to restore FD " << rfd;
          _close(backup_rfd);
          callback(boost::asio::error::broken_pipe, SIGTERM);
        });
      }
    };
    // Replace file descriptor
    RAY_CHECK(_dup2(rpipefd, rfd) != -1) << "Failed to replace FD " << rfd;
    _close(rpipefd);
    stream->async_wait(*do_read);
  });
#else
  RAY_LOG(FATAL) << "Not implemented";
#endif
}

// Copyright 2020 The Ray Authors.
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

#include "ray/util/util.h"

#include <stdio.h>
#include <stdlib.h>
#ifndef _WIN32
#include <sys/un.h>
#endif

#include <algorithm>
#include <boost/asio/generic/stream_protocol.hpp>
#include <sstream>
#include <string>
#include <vector>
#ifndef _WIN32
#include <boost/asio/local/stream_protocol.hpp>
#endif
#include <boost/asio/ip/tcp.hpp>

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
  // The if statement below may be incorrect. See:
  // https://github.com/ray-project/ray/pull/10131#discussion_r473871563
  if (s.empty()) {
    return {};
  }
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

std::shared_ptr<absl::flat_hash_map<std::string, std::string>> ParseURL(std::string url) {
  auto result = std::make_shared<absl::flat_hash_map<std::string, std::string>>();
  std::string delimiter = "?";
  size_t pos = 0;
  pos = url.find(delimiter);
  if (pos == std::string::npos) {
    return result;
  }

  const std::string base_url = url.substr(0, pos);
  result->emplace("url", base_url);
  url.erase(0, pos + delimiter.length());
  const std::string query_delimeter = "&";

  auto parse_key_value_with_equal_delimter = [](std::string key_value) {
    // Parse the query key value pair.
    const std::string key_value_delimter = "=";
    size_t key_value_pos = 0;
    key_value_pos = key_value.find(key_value_delimter);
    const std::string key = key_value.substr(0, key_value_pos);
    return std::make_pair(key, key_value.substr(key.size() + 1));
  };

  while ((pos = url.find(query_delimeter)) != std::string::npos) {
    std::string token = url.substr(0, pos);
    auto key_value_pair = parse_key_value_with_equal_delimter(token);
    result->emplace(key_value_pair.first, key_value_pair.second);
    url.erase(0, pos + delimiter.length());
  }
  std::string token = url.substr(0, pos);
  auto key_value_pair = parse_key_value_with_equal_delimter(token);
  result->emplace(key_value_pair.first, key_value_pair.second);
  return result;
}

namespace ray {

bool IsRayletFailed(const std::string &raylet_pid) {
  auto should_shutdown = false;
  if (!raylet_pid.empty()) {
    auto pid = static_cast<pid_t>(std::stoi(raylet_pid));
    if (!IsProcessAlive(pid)) {
      should_shutdown = true;
    }
  } else if (!IsParentProcessAlive()) {
    should_shutdown = true;
  }
  return should_shutdown;
}

void QuickExit() {
  ray::RayLog::ShutDownRayLog();
  _Exit(1);
}

}  // namespace ray

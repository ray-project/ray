// Copyright 2025 The Ray Authors.
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

#include "ray/util/cmd_line_utils.h"

#include <string>
#include <vector>

#include "ray/util/logging.h"
#include "ray/util/string_utils.h"

namespace {

/// Python analog: shlex.join(args)
std::string CreatePosixCommandLine(const std::vector<std::string> &args) {
  std::string result;
  const std::string safe_chars("%*[-A-Za-z0-9%_=+]");
  const char single_quote = '\'';
  for (size_t a = 0; a != args.size(); ++a) {
    std::string arg = args[a], arg_with_null = arg + '\0';
    std::string::const_iterator i = arg_with_null.begin();
    if (ray::ScanToken(i, safe_chars) != arg) {
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
    if (!in_dquotes && (i >= j || ray::ScanToken(i, "%*[ \t]").size())) {
      result.push_back(arg);
      arg.clear();
    }
    stop |= i >= j && !in_dquotes;
    arg += ray::ScanToken(i, in_dquotes ? "%*[^\\\"]" : "%*[^\\\" \t]");
    std::string possible_escape = ray::ScanToken(i, "%*[\\]");
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

// Python analog: subprocess.list2cmdline(args)
static std::string CreateWindowsCommandLine(const std::vector<std::string> &args) {
  std::string result;
  const std::string safe_chars("%*[-A-Za-z0-9%_=+]");
  const char double_quote = '\"';
  for (size_t a = 0; a != args.size(); ++a) {
    std::string arg = args[a], arg_with_null = arg + '\0';
    std::string::const_iterator i = arg_with_null.begin();
    if (ray::ScanToken(i, safe_chars) != arg) {
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

}  // namespace

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

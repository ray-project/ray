#include "ray/util/util.h"

#include <algorithm>
#include <regex>
#include <string>
#include <stdio.h>
#include <vector>

#include "ray/util/logging.h"

std::string ScanToken(std::string::const_iterator &c_str, std::string format) {
  int i = 0;
  std::string result;
  format += "%n";
  if (static_cast<size_t>(sscanf(&*c_str, format.c_str(), &i)) <= 1) {
    result.insert(result.end(), c_str, c_str + i);
    c_str += i;
  }
  return result;
}

static std::vector<std::string> ParsePosixCommandLine(const std::string &s) {
  RAY_CHECK(s.find('\0') >= s.size()) << "Invalid null character in command line";
  // See the unit-tests for examples. To compare against the platform's behavior, try:
  // $ python3 -c "import sys; [print(a) for a in sys.argv[1:]]" \x\\y "\x\\y" '\x\\y'
  std::vector<std::string> result;
  std::string arg, c_str = s + '\0';
  std::string::const_iterator i = c_str.begin(), j = c_str.end() - 1;
  for (bool stop = false; !stop;) {
    if (i >= j || *i == ' ' || *i == '\t') {
      if (i > c_str.begin()) {
        result.push_back(arg);
      }
      arg.clear();
      ScanToken(i, "%*[ \t]");  // skip extra whitespace
      stop |= i >= j;
    } else if (*i == '\'') {
      arg += ScanToken(++i, "%*[^\']");
      ScanToken(i, "%*1[\']");
    } else if (*i == '\\') {
      ++i;
      arg += i < j ? *i++ : *(i - 1);  // remove backslash, but only if it's not at EOF
    } else if (*i == '\"') {
      ++i;
      while (i < j && *i != '\"') {
        arg += ScanToken(i, "%*[^\\\"]");
        std::string t = ScanToken(i, "\\%*1c");  // scan escape sequence
        if (t.size() > 1 && (t.back() == '\\' || t.back() == '\"')) {
          t.erase(0, 1);  // remove backslash
        }
        arg += t;
      }
      ScanToken(i, "\"");
    } else {
      arg += *i++;
    }
  }
  return result;
}

static std::vector<std::string> ParseWindowsCommandLine(const std::string &s) {
  RAY_CHECK(s.find('\0') >= s.size()) << "Invalid null character in command line";
  // See the unit-tests for examples. To compare against the platform's behavior, try:
  // > python3 -c "import sys; [print(a) for a in sys.argv[1:]]" \x\\y "\x\\y"
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
#include "ray/util/util.h"

#include <algorithm>
#include <regex>
#include <string>
#include <vector>

#include "ray/util/logging.h"

static std::vector<std::string> ParsePosixCommandLine(const std::string &s) {
  // See the unit-tests for examples. To compare against the platform's behavior, try:
  //  $ python3 -c "import sys; [print(a) for a in sys.argv[1:]]" \x\\y "\x\\y" '\x\\y'
  std::vector<std::string> result;
  const std::string
      // Single-quoted strings lack single-quotes
      single_quoted = "'([^']*)'",
      // Double-quoted strings lack double-quotes, except after unescaped backslashes
      double_quoted = R"(\"((?:[^"\\]|\\.)*)\")",
      // Unquoted tokens lack spaces & quotes, except after unescaped backslashes
      unquoted = R"(((?:\\(?:.|$)|[^"'\s\\])+))",
      // Whitespace & EOF are delimiters
      boundary = R"(\s+|$)";
  std::regex re_cmdline(single_quoted + "|" + double_quoted + "|" + unquoted + "|" +
                        boundary);
  std::regex re_dq_escape(R"(\\([\\"]))"), re_escape(R"(\\(.))");
  bool was_space = true;
  std::string arg;
  for (std::sregex_iterator i(s.begin(), s.end(), re_cmdline), end; i != end; ++i) {
    const std::smatch &groups = *i;
    bool space = false;
    if (groups[1].matched) {
      // single_quoted: No escaping: '\x\\y' is just \x\\y verbatim
      arg += groups[1];
    } else if (groups[2].matched) {
      // double_quoted: Backslashes stay if they don't escape anything: "\x\\y" == '\x\y'
      arg += std::regex_replace(groups[2].str(), re_dq_escape, "$1");
    } else if (groups[3].matched) {
      // unquoted: Backslashes get removed if they don't escape anything: \x\\y == 'x\y'
      arg += std::regex_replace(groups[3].str(), re_escape, "$1");
    } else {
      // boundary
      if (!was_space) {
        // The previous token was an argument we just finished, not a contiguous delimiter
        result.push_back(arg);
        arg.clear();
      }
      space = true;
    }
    was_space = space;
  }
  return result;
}

static std::vector<std::string> ParseWindowsCommandLine(const std::string &s) {
  // See the unit-tests for examples. To compare against the platform's behavior, try:
  //  > python3 -c "import sys; [print(a) for a in sys.argv[1:]]" \x\\y "\x\\y"
  std::vector<std::string> result;
  const std::string
      // double-quoted tokens lack double-quotes, except after unescaped backslashes
      double_quoted = R"(\"((?:[^"\\]|\\.)*)\")",
      // Unquoted tokens lack spaces & quotes, except after unescaped backslashes
      unquoted = R"(((?:\\(?:.|$)|[^"\s\\])+))",
      // Whitespace & EOF are delimiters
      boundary = R"(\s+|$)";
  std::regex re_cmdline(double_quoted + "|" + unquoted + "|" + boundary);
  std::regex re_dq_escape(R"((\\*)\1(?:\\(")|$))"), re_escape(R"((\\*)\1\\("))");
  bool was_space = false;
  std::string arg;
  for (std::sregex_iterator i(s.begin(), s.end(), re_cmdline), end; i != end; ++i) {
    const std::smatch &groups = *i;
    bool space = false;
    if (groups[1].matched) {
      // double_quoted: Backslashes are escapes only if they precede a double-quote
      arg += std::regex_replace(groups[1].str(), re_dq_escape, "$1$2");
    } else if (groups[2].matched) {
      // unquoted: Backslashes are escapes only if they precede a double-quote
      arg += std::regex_replace(groups[2].str(), re_escape, "$2");
    } else {
      // boundary
      if (!was_space) {
        // The previous token was an argument we just finished, not a contiguous delimiter
        result.push_back(arg);
        arg.clear();
      }
      space = true;
    }
    was_space = space;
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
  std::regex unsafe_chars(R"([^[:alnum:]%_=+\-])"), single_quote("(')");
  for (size_t i = 0; i != args.size(); ++i) {
    std::string arg = args[i];
    if (std::regex_search(arg, unsafe_chars)) {
      // Prefer single-quotes. Double-quotes have unpredictable behavior, e.g. for "\!".
      arg = "'" + std::regex_replace(arg, single_quote, "'\\$1'") + "'";
    }
    if (i > 0) {
      result += ' ';
    }
    result += arg;
  }
  return result;
}

static std::string CreateWindowsCommandLine(const std::vector<std::string> &args) {
  std::string result;
  std::regex unsafe_chars(R"([^[:alnum:]%_=+\-:])"), double_quote(R"((\\*)("))"),
      end(R"((\\*)$)");
  for (size_t i = 0; i != args.size(); ++i) {
    std::string arg = args[i];
    if (std::regex_search(arg, unsafe_chars)) {
      // Escape only backslashes that precede double-quotes
      arg = std::regex_replace(arg, end, "$1$1");
      arg = '"' + std::regex_replace(arg, double_quote, "$1$1\\$2") + '"';
    }
    if (i > 0) {
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
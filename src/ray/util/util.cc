#include "ray/util/util.h"

#include <algorithm>
#include <regex>
#include <string>
#include <vector>

#include "ray/util/logging.h"

static std::vector<std::string> ParsePosixCommandLine(const std::string &s) {
  std::vector<std::string> result;
  const std::string single_quoted = R"('([^']*)')",
                    double_quoted = R"(\"((?:[^"\\]|\\.)*)\")",
                    unquoted = R"(((?:\\(?:.|$)|[^"'\s\\])+))", boundary = R"(\s+|$)";
  std::regex re_cmdline(single_quoted + "|" + double_quoted + "|" + unquoted + "|" +
                        boundary),
      re_dq_escape(R"(\\([\\"]))"), re_escape(R"(\\(.))");
  bool was_space = true;
  std::string arg;
  for (std::sregex_iterator i(s.begin(), s.end(), re_cmdline), end; i != end; ++i) {
    const std::smatch &match = *i;
    bool space = false;
    size_t j = 0;
    if (match[++j].matched) {
      arg += match[j];
    } else if (match[++j].matched) {
      arg += std::regex_replace(match[j].str(), re_dq_escape, "$1");
    } else if (match[++j].matched) {
      arg += std::regex_replace(match[j].str(), re_escape, "$1");
    } else {
      if (!was_space) {
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
  std::vector<std::string> result;
  const std::string double_quoted = R"(\"((?:[^"\\]|\\.)*)\")",
                    unquoted = R"(((?:\\(?:.|$)|[^"\s\\])+))", boundary = R"(\s+|$)";
  std::regex re_cmdline(double_quoted + "|" + unquoted + "|" + boundary),
      re_dq_escape(R"((\\*)\1(?:\\(")|$))"), re_escape(R"((\\*)\1\\("))");
  bool was_space = false;
  std::string arg;
  for (std::sregex_iterator i(s.begin(), s.end(), re_cmdline), end; i != end; ++i) {
    const std::smatch &match = *i;
    bool space = false;
    size_t j = 0;
    if (match[++j].matched) {
      arg += std::regex_replace(match[j].str(), re_dq_escape, "$1$2");
    } else if (match[++j].matched) {
      arg += std::regex_replace(match[j].str(), re_escape, "$2");
    } else if (!was_space) {
      result.push_back(arg);
      arg.clear();
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
      arg = std::regex_replace(arg, single_quote, "'\\$1'");
      arg = "'" + arg + "'";
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
      arg = std::regex_replace(arg, end, "$1$1");
      arg = std::regex_replace(arg, double_quote, "$1$1\\$2");
      arg = '"' + arg + '"';
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
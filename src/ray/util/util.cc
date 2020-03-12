#include "ray/util/util.h"

#include <algorithm>
#include <string>
#include <vector>

#include "ray/util/logging.h"

std::vector<std::string> ParseCommandLine(const std::string &s, CommandLineSyntax kind) {
  using std::min;
  if (s.find('\0') < s.size()) {
    RAY_LOG(ERROR) << "command-line arguments cannot contain null characters: " << s;
  }
  if (kind == CommandLineSyntax::System) {
#ifdef _WIN32
    kind = CommandLineSyntax::Windows;
#else
    kind = CommandLineSyntax::POSIX;
#endif
  }
  enum State {
    Normal,
    AfterWhitespace,
    AfterBackslash,
    InSingleQuotes,
    InDoubleQuotes,
    AfterBackslashInDoubleQuotes,
  } state = kind == CommandLineSyntax::Windows ? State::Normal : State::AfterWhitespace;
  std::vector<std::string> result;
  std::string arg;
  for (size_t i = 0; i < s.size(); ++i) {
    char ch = s[i];
    switch (state) {
    case State::Normal:
    case State::AfterWhitespace:
      switch (ch) {
      case ' ':
      case '\t':
      case '\n':
        if (state != State::AfterWhitespace) {
          result.push_back(arg);
          arg.clear();
        }
        state = State::AfterWhitespace;
        break;
      case '\\':
        arg += ch;  // Keep backslash for now; we'll remove it if needed
        state = State::AfterBackslash;
        break;
      case '\'':
        if (kind != CommandLineSyntax::POSIX) {
          goto DEFAULT_AFTER_WHITESPACE;
        }
        state = State::InSingleQuotes;
        break;
      case '\"':
        state = State::InDoubleQuotes;
        break;
      default:
      DEFAULT_AFTER_WHITESPACE:
        arg += ch;
        state = State::Normal;
        break;
      }
      break;
    case State::AfterBackslash:
      if (kind == CommandLineSyntax::POSIX ||
          (kind == CommandLineSyntax::Windows && ch == '\"')) {
        arg.pop_back();  // Remove preceding backslash
      }
      arg += ch;
      state = State::Normal;
      break;
    case State::InSingleQuotes:
      switch (ch) {
      case '\'':
        state = State::Normal;
        break;
      default:
        arg += ch;
        break;
      }
      break;
    case State::InDoubleQuotes:
      switch (ch) {
      case '\\':
        arg += ch;  // Keep backslash for now; we'll remove it if needed
        state = State::AfterBackslashInDoubleQuotes;
        break;
      case '\"':
        if (kind == CommandLineSyntax::Windows) {
          size_t j = arg.find_last_not_of('\\');
          j = j < arg.size() ? j + 1 : 0;
          arg.erase(j, (arg.size() - j) / 2);
        }
        state = State::Normal;
        break;
      default:
        arg += ch;
        break;
      }
      break;
    case State::AfterBackslashInDoubleQuotes:
      switch (ch) {
      case '\\':
        if (kind == CommandLineSyntax::POSIX) {
          arg.pop_back();  // Remove preceding backslash
        }
        break;
      case '\"':
        arg.pop_back();  // Remove preceding backslash
        break;
      }
      arg += ch;
      state = State::InDoubleQuotes;
      break;
    }
  }
  if (state != State::AfterWhitespace) {
    result.push_back(arg);
    arg.clear();
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
  for (size_t i = 0; i != args.size(); ++i) {
    if (i > 0) {
      result += ' ';
    }
    std::string arg = args[i];
    bool is_safe_without_quotes = true;
    for (size_t j = 0; j != arg.size(); ++j) {
      char ch = arg[j];
      switch (ch) {
      case '%':
      case '-':
      case '_':
      case '=':
      case '+':
        break;
      case ':':
        if (kind == CommandLineSyntax::Windows) {
          is_safe_without_quotes = false;
        }
        break;
      default:
        if (!isalnum(ch)) {
          is_safe_without_quotes = false;
        }
        break;
      }
    }
    if (!is_safe_without_quotes) {
      char quote = kind == CommandLineSyntax::POSIX ? '\'' : '\"';
      bool quote_started = false;
      for (size_t j = 0; j != arg.size(); ++j) {
        char ch = arg[j];
        if (ch == quote) {
          if (quote_started) {
            result += quote;
            quote_started = !quote_started;
          }
          result += '\\';
          result += ch;
        } else {
          if (!quote_started) {
            result += quote;
            quote_started = !quote_started;
          }
          if (quote == '\"' && ch == '\\') {
            result += '\\';
            result += ch;
          } else {
            result += ch;
          }
        }
      }
      if (quote_started) {
        result += quote;
        quote_started = !quote_started;
      }
    } else {
      result += arg;
    }
  }
  return result;
}
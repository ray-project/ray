#ifndef RAY_UTIL_COMMAND_LINE_ARGS_H
#define RAY_UTIL_COMMAND_LINE_ARGS_H

#include <string>
#include <unordered_map>

namespace ray {

/// A simple command line arguments util.
///
/// It requires the options of command are as pair, like:
///     ./main --enable_x true --disable_y "false"
/// If the value of one option is empty, it should be specified a empty string, like:
///     ./main --enable_x ""
class CommandLineArgs final {
 public:
  /// Constructor. Note that the `argc` must equals the size of `argv` array.
  CommandLineArgs(int args, char **argv);

  /// Get the program name of this command line statement.
  std::string GetProgramName() const;

  /// Whether this command line statement contains the option `key`.
  bool Contains(const std::string &key) const;

  /// Get the option value of the given `key`.
  ///
  /// Note that it will check fail if this command line statement doesn't contains the
  /// `key`.
  std::string Get(const std::string &key) const;

  /// Get the option value of the given `key`.
  ///
  /// Note that it will return the `default_value` if this command line statement doesn't
  /// contains the `key`.
  std::string Get(const std::string &key, const std::string &default_value) const;

 private:
  /// The name of this program.
  std::string program_name_;
  /// The arguments of this command line.
  std::unordered_map<std::string, std::string> arguments_;
};

}  // namespace ray

#endif  // RAY_UTIL_COMMAND_LINE_ARGS_H

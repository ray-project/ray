#include "ray/util/command_line_args.h"
#include "ray/util/logging.h"

namespace ray {

CommandLineArgs::CommandLineArgs(int argc, char **argv) {
  RAY_CHECK(argc > 0) << "Number of command line arguments must be greater than 0.";
  program_name_ = argv[0];
  for (size_t i = 1; i < argc; i += 2) {
    RAY_CHECK(nullptr != argv[i]);
    const std::string key(argv[i]);
    if (key.size() <= 2 || "--" != key.substr(0, 2)) {
      RAY_LOG(FATAL) << "The option must be started with `--`";
    }

    std::string value(argv[i + 1]);
    RAY_CHECK(value.size() >= 0) << "The argument must be as pair like: "
                                 << "--skip true";
    if (value.front() == '\"') {
      RAY_CHECK(value.size() >= 2 && value.back() == '\"')
          << "If the argument starts with \", it also must end with \".";
      value = value.substr(1, value.size() - 2);
    }
    arguments_.emplace(key.substr(2, key.size() - 2), value);
  }
}

std::string CommandLineArgs::GetProgramName() const { return program_name_; }

bool CommandLineArgs::Contains(const std::string &key) const {
  return arguments_.end() != arguments_.find(key);
}

std::string CommandLineArgs::Get(const std::string &key) const {
  RAY_CHECK(Contains(key));
  return arguments_.at(key);
}

std::string CommandLineArgs::Get(const std::string &key,
                                 const std::string &default_value) const {
  if (Contains(key)) {
    return arguments_.at(key);
  }

  return default_value;
}

}  // namespace ray

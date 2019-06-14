#ifndef RAY_CONFIG_H
#define RAY_CONFIG_H

#include <sstream>
#include <unordered_map>

#include "ray/util/logging.h"

class RayConfig {
/// -----------Include ray_config_def.h to define config items.----------------
/// A helper macro that defines a config item.
/// In particular, this generates a private field called `name_` and a public getter
/// method called `name()` for a given config item.
///
/// \param type Type of the config item.
/// \param name Name of the config item.
/// \param default_value Default value of the config item.
#define RAY_CONFIG(type, name, default_value) \
 private:                                     \
  type name##_ = default_value;               \
                                              \
 public:                                      \
  inline type name() { return name##_; }

#include "ray_config_def.h"
/// -------------------------------------------------------------------------
#undef RAY_CONFIG

 public:
  static RayConfig &instance() {
    static RayConfig config;
    return config;
  }

// clang-format off
/// -----------Include ray_config_def.h to set config items.-------------------
/// A helper macro that helps to set a value to a config item.
#define RAY_CONFIG(type, name, default_value) \
  if (pair.first == #name) {                  \
    std::istringstream stream(pair.second);   \
    stream >> name##_;                        \
    continue;                                 \
  }

  void initialize(const std::unordered_map<std::string, std::string> &config_map) {
    RAY_CHECK(!initialized_);
    for (auto const &pair : config_map) {
      // We use a big chain of if else statements because C++ doesn't allow
      // switch statements on strings.
#include "ray_config_def.h"
      RAY_LOG(FATAL) << "Received unexpected config parameter " << pair.first;
    }
    initialized_ = true;
  }
/// ---------------------------------------------------------------------
#undef RAY_CONFIG

  /// Whether the initialization of the instance has been called before.
  /// The RayConfig instance can only (and must) be initialized once.
  bool initialized_ = false;
};
// clang-format on

#endif  // RAY_CONFIG_H

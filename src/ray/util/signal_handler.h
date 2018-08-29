#ifndef RAY_UTIL_SIGNAL_HANDLER_H
#define RAY_UTIL_SIGNAL_HANDLER_H

#include <signal.h>
#include <string>
#include <vector>

namespace ray {

class SignalHandlers {
 public:
  /// Setup the signal handler, which should be called in main function.
  ///
  /// \param app_name The app name that install the signal handler.
  /// \param install_sigterm Whether install the handler for SIGTERM, because
  /// some app has already have a handler for this signal.
  /// \return Void.
  static void InstallSignalHandler(const std::string &app_name,
                                   bool is_installing_sigterm);

  /// Reset the signal handler to the default handler.
  ///
  /// \return Void.
  static void UninstallSignalHandler();

  /// Get the app name.
  ///
  /// \return The returned app name.
  static std::string GetAppName();

  /// Get the logging level for termination signal.
  ///
  /// \return The logging level.
  static int GetLoggingLevel();

 private:
  static std::string app_name_;
  static int terminate_logging_level_;
  static std::vector<int> installed_signals_;
  // Disable constructor.
  SignalHandlers() = delete;
  SignalHandlers(const SignalHandlers &) = delete;
};

}  // namespace ray

#endif  // RAY_UTIL_SIGNAL_HANDLER_H

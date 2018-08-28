#ifndef RAY_UTIL_SIGNAL_HANDLER_H
#define RAY_UTIL_SIGNAL_HANDLER_H

#include <string>
#include <vector>

namespace ray {

class SignalHandler {
 public:
  /// Setup the signal handler, which should be called in main function.
  ///
  /// \param app_name The app name that install the signal handler.
  /// \param install_sigterm Whether install the handler for SIGTERM, because
  /// some app has already have a handler for this signal.
  /// \return Void.
  static void InstallSingalHandler(const std::string &app_name, bool install_sigterm);

  /// Reset the signal handler to the default handler.
  ///
  /// \return Void.
  static void UninstallSingalHandler();

 private:
  static void FatalErrorHandler(int signal);
  static void TerminateHandler(int signal);
  static void InstallSignalHandlerHelper(int signal, void (*handler)(int));
  static std::string GetReachDebugInfo(int signal);
  static std::string app_name_;
  static int terminate_logging_level_;
  static std::vector<int> installed_signals_;
};

}  // namespace ray

#endif  // RAY_UTIL_SIGNAL_HANDLER_H

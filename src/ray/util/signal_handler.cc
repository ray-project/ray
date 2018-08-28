#include "ray/util/signal_handler.h"
#include "ray/util/logging.h"

#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>

using namespace ray;

// Normally stop ray will also send the signal.
int SignalHandlers::terminate_logging_level_ = RAY_INFO;
// The list of signals that has installed handlers.
std::vector<int> SignalHandlers::installed_signals_;
// The current app name.
std::string SignalHandlers::app_name_;

void SignalHandlers::InstallSignalHandlerHelper(int sig, void (*handler)(int)) {
  signal(sig, handler);
  installed_signals_.push_back(sig);
}
void SignalHandlers::InstallSingalHandler(const std::string &app_name,
                                          bool is_installing_sigterm) {
  app_name_ = app_name;
  // SIGINT = 2. It is the message of: Ctrl + C.
  InstallSignalHandlerHelper(SIGINT, TerminateHandler);
  // SIGILL = 4. It is the message when using *(nullptr).
  InstallSignalHandlerHelper(SIGILL, FatalErrorHandler);
  // SIGSEGV = 11. It is the message when segment fault happens.
  InstallSignalHandlerHelper(SIGSEGV, FatalErrorHandler);
  if (is_installing_sigterm) {
    // SIGTERM = 15. Termination message.
    // Here is a special treatment for this signal, because
    // this message handler is used by local_scheduler and global_scheduler.
    InstallSignalHandlerHelper(SIGTERM, TerminateHandler);
  }
  // Do not set handler for SIGABRT which happens when abort() is called.
  // If we set handler for SIGABRT, there will be indefinite call.
}

void SignalHandlers::UninstallSingalHandler() {
  for (auto sig : installed_signals_) {
    signal(sig, SIG_DFL);
  }
  installed_signals_.clear();
}

void SignalHandlers::FatalErrorHandler(int sig) {
  if (sig == SIGILL || sig == SIGSEGV) {
    auto info = GetRichDebugInfo(sig);
    RAY_LOG(FATAL) << info;
  }
}

void SignalHandlers::TerminateHandler(int sig) {
  if (RayLog::IsLevelEnabled(terminate_logging_level_) &&
      (sig == SIGINT || sig == SIGTERM)) {
    auto info = GetRichDebugInfo(sig);
    RAY_LOG(FATAL) << info;
  }
}

std::string SignalHandlers::GetRichDebugInfo(int sig) {
  std::ostringstream ostream;
  const size_t max_path_length = 256;
  char working_directory[max_path_length] = "";
  auto p = getcwd(working_directory, max_path_length);
  RAY_IGNORE_EXPR(p);
  ostream << "Signal: " << sig << " received for app: " << app_name_ << "\n";
  ostream << "Current working directory: " << working_directory << "\n";
  return ostream.str();
}

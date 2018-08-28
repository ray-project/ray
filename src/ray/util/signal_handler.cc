#include "ray/util/signal_handler.h"
#include "ray/util/logging.h"

#include <sstream>
#include <stdlib.h>
#include <unistd.h>

using namespace ray;

// Normally stop ray will also send the signal.
int SignalHandler::ignorable_logging_level_ = RAY_INFO;
// The list of signals that has installed handlers.
std::vector<int> SignalHandler::installed_signals_;
// The current app name.
std::string SignalHandler::app_name_;

void SignalHandler::InstallSignalHandlerHelper(int sig, void(*handler)(int)){
    signal(sig, handler);
    installed_signals_.push_back(sig);
}
void SignalHandler::InstallSingalHandler(const std::string &app_name, bool install_sigterm) {
  app_name_ = app_name;
  // SIGINT = 2. It is the message of: Ctrl + C.
  InstallSignalHandlerHelper(SIGINT, IgnorableHandler);
  // SIGILL = 4. It is the message when using *(nullptr).
  InstallSignalHandlerHelper(SIGILL, UnignorableHandler);
  // SIGSEGV = 11. It is the message when segment fault happens.
  InstallSignalHandlerHelper(SIGSEGV, UnignorableHandler);
  if (install_sigterm) {
    // SIGTERM = 15. Termination message.
    // Here is a special treatment for this signal, because
    // this message handler is used by local_scheduler and global_scheduler.
    InstallSignalHandlerHelper(SIGTERM, IgnorableHandler);
  }
  // Do not set handler for SIGABRT which happens when abort() is called.
  // If we set handler for SIGABRT, there will be indefinite call.
}

void SignalHandler::UninstallSingalHandler() {
  for (auto sig : installed_signals_) {
    signal(sig, SIG_DFL);
  }
}

void SignalHandler::UnignorableHandler(int sig) {
  if (sig == SIGILL || sig == SIGSEGV) {
    auto info = GetReachDebugInfo(sig);
    RAY_LOG(FATAL) << info;
  }
}

void SignalHandler::IgnorableHandler(int sig) {
  if (RayLog::IsLevelEnabled(ignorable_logging_level_)
      && (sig == SIGINT || sig == SIGTERM)) {
    auto info = GetReachDebugInfo(sig);
    RAY_LOG(FATAL) << info;
  }
}

std::string SignalHandler::GetReachDebugInfo(int sig) {
  std::ostringstream ostream;
  const int max_path_length = 256;
  char working_directory[max_path_length] = "";
  getcwd(working_directory, max_path_length);
  ostream << "Signal: " << sig << " received for app: " << app_name_ << "\n";
  ostream << "Current working directory: " << working_directory << "\n";
  return ostream.str();
}

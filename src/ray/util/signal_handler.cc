#include "ray/util/signal_handler.h"

#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>

#include "ray/util/logging.h"

using namespace ray;

// Normally stop ray will also send the signal.
int SignalHandlers::terminate_logging_level_ = RAY_INFO;
// The list of signals that has installed handlers.
std::vector<int> SignalHandlers::installed_signals_;
// The current app name.
std::string SignalHandlers::app_name_;

static void InstallSignalHandlerHelper(int signal, const struct sigaction &action,
                                       std::vector<int> *installed_signals) {
  sigaction(signal, &action, nullptr);
  if (installed_signals) {
    installed_signals->push_back(signal);
  }
}

static std::string GetRichDebugInfo(int sig) {
  std::ostringstream ostream;
  char working_directory[PATH_MAX] = "";
  auto p = getcwd(working_directory, PATH_MAX);
  RAY_IGNORE_EXPR(p);
  ostream << "Signal: " << sig << " received for app: " << SignalHandlers::GetAppName();
  ostream << "\nCurrent working directory: " << working_directory << "\n";
  return ostream.str();
}

static void FatalErrorHandler(int sig) {
  if (sig == SIGILL || sig == SIGSEGV) {
    const auto &info = GetRichDebugInfo(sig);
    RAY_LOG(FATAL) << info;
  }
}

static void TerminateHandler(int sig) {
  if (RayLog::IsLevelEnabled(SignalHandlers::GetLoggingLevel()) &&
      (sig == SIGINT || sig == SIGTERM)) {
    auto info = GetRichDebugInfo(sig);
    RAY_LOG(FATAL) << info;
  }
}

std::string SignalHandlers::GetAppName() { return app_name_; }

int SignalHandlers::GetLoggingLevel() { return terminate_logging_level_; }

void SignalHandlers::InstallSignalHandler(const std::string &app_name,
                                          bool is_installing_sigterm) {
  app_name_ = app_name;
  struct sigaction terminate_action;
  terminate_action.sa_handler = TerminateHandler;
  struct sigaction fatal_action;
  fatal_action.sa_handler = FatalErrorHandler;
  // SIGINT = 2. It is the message of: Ctrl + C.
  InstallSignalHandlerHelper(SIGINT, terminate_action, &installed_signals_);
  // SIGILL = 4. It is the message when using *(nullptr).
  InstallSignalHandlerHelper(SIGILL, fatal_action, &installed_signals_);
  // SIGSEGV = 11. It is the message when segment fault happens.
  InstallSignalHandlerHelper(SIGSEGV, fatal_action, &installed_signals_);
  if (is_installing_sigterm) {
    // SIGTERM = 15. Termination message.
    // Here is a special treatment for this signal, because
    // this message handler is used by local_scheduler and global_scheduler.
    InstallSignalHandlerHelper(SIGTERM, terminate_action, &installed_signals_);
  }
  // Do not set handler for SIGABRT which happens when abort() is called.
  // If we set handler for SIGABRT, there will be indefinite call.
}

void SignalHandlers::UninstallSignalHandler() {
  struct sigaction restore_action;
  restore_action.sa_handler = SIG_DFL;
  for (auto signal : installed_signals_) {
    sigaction(signal, &restore_action, nullptr);
  }
  installed_signals_.clear();
}

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <atomic>
#include <csignal>
#include <string>
#include <vector>

#include "src/ray/common/status.h"
#include "src/ray/gcs/gcs_server/gcs_server.h"

namespace {
std::atomic<bool> should_exit{false};

void SignalHandler(int signum) {
  std::cout << "\nReceived signal " << signum << ". Shutting down...\n";
  should_exit = true;
}
}  // namespace

int main(int argc, char *argv[]) {
  std::vector<std::string> args(argv, argv + argc);
  if (args.size() != 3) {
    std::cerr << "Usage: " << args[0] << " <address> <port>\n";
    return 1;
  }

  const std::string& address = args[1];
  const int port = std::stoi(args[2]);

  // Set up signal handlers
  if (signal(SIGINT, SignalHandler) == SIG_ERR) {
    std::cerr << "Failed to set up SIGINT handler\n";
    return 1;
  }
  if (signal(SIGTERM, SignalHandler) == SIG_ERR) {
    std::cerr << "Failed to set up SIGTERM handler\n";
    return 1;
  }

  // Create GCS server
  auto gcs_server = std::make_unique<ray::gcs::GcsServer>(address, port);

  // Start GCS server
  auto status = gcs_server->Start();
  if (!status.ok()) {
    std::cerr << "Failed to start GCS server: " << status.ToString() << "\n";
    return 1;
  }

  std::cout << "GCS server started on " << address << ":" << port << "\n";
  std::cout << "Press Ctrl+C to stop...\n";

  // Main event loop
  while (!should_exit) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Graceful shutdown
  std::cout << "Shutting down GCS server...\n";
  status = gcs_server->Stop();
  if (!status.ok()) {
    std::cerr << "Failed to stop GCS server: " << status.ToString() << "\n";
    return 1;
  }

  std::cout << "GCS server stopped successfully\n";
  return 0;
} 
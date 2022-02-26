#pragma once
#include "ray/core_worker/core_worker_options.h"

class Config {
 public:
  std::string bootstrap_ip = "";
  int bootstrap_port = 6379;
  std::string redis_password = "5241590000000000";
  std::string driver_name;
  std::string session_dir;
  std::vector<std::string> code_search_path;

  // The command line args to be appended as parameters of the `ray start` command. It
  // takes effect only if Ray head is started by a driver. Run `ray start --help` for
  // details.
  std::vector<std::string> head_args = {};
};

void InitOptions(Config* config,
                ray::core::CoreWorkerOptions* options,
                std::string code_search_path,
                std::string head_args,
                int argc, char **argv);

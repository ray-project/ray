#pragma once
#include "ray/gcs/gcs_client/global_state_accessor.h"

std::string GetNodeIpAddress(const std::string &address = "8.8.8.8:53");

void StartRayNode(const int redis_port, const std::string redis_password,
                                 const std::vector<std::string> &head_args);
void StopRayNode();

std::unique_ptr<ray::gcs::GlobalStateAccessor> CreateGlobalStateAccessor(
    const std::string &redis_address, const std::string &redis_password);

std::unique_ptr<ray::gcs::GlobalStateAccessor> CreateGlobalStateAccessor(
    const std::string &bootstrap_address);

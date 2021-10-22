// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/stats/metric.h"

DEFINE_stats(grpc_client_req_latency_ms, "Request latency in grpc call",
             ("Method"), (), ray::stats::GAUGE);
DEFINE_stats(grpc_client_req_new, "New request number in grpc client", ("Method"), (),
             ray::stats::COUNT);
DEFINE_stats(grpc_client_req_finished, "Finished request number in grpc client",
             ("Method"), (), ray::stats::COUNT);

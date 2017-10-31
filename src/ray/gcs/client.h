// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <string>
#include <map>

#include "ray/id.h"
#include "ray/status.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class AsyncGCSClient {
 public:

  AsyncGCSClient();
  ~AsyncGCSClient();

  Status Connect(const std::string& address, int port);

  inline GCSTable<FunctionID, FunctionTableData>& function_table();
  inline GCSTable<TaskID, ErrorTableData>& error_table();
  // TODO: Some API for getting the error on the driver
  inline GCSTable<ClassID, ClassTableData>& class_table();
  inline GCSTable<ActorID, ActorTableData>& actor_table();
  inline GCSTable<ClassID, CustomSerializerData>& custom_serializer_table();
  inline GCSTable<ConfigID, ConfigTableData>& config_table();
  inline ObjectTable& object_table();
  inline TaskTable& task_table();
  inline EventTable& event_table();

  // We also need something to export generic code to run on workers from the driver (to set the PYTHONPATH)

  Status AddExport(const std::string& driver_id, std::string& export_data);
  Status GetExport(const std::string& driver_id, int64_t export_index, done_callback);
}

class SyncGSCClient {
    Status LogEvent(const std::string& key, const std::string& value, double timestamp);
    Status NotifyError(const std::map<std::string, std::string>& error_info);
    Status RegisterFunction(const JobID& job_id, const FunctionID& function_id, const std::string& language, const std::string& name, const std::string& data);
    Status RetrieveFunction(const JobID& job_id, const FunctionID& function_id, std::string* name, std::string* data);

    Status AddExport(const std::string& driver_id, std::string& export_data);
    Status GetExport(const std::string& driver_id, int64_t export_index, std::string* data);
};

}  // namespace gcs

}  // namespace ray

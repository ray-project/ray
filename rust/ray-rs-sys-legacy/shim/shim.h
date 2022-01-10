
using ray::core::CoreWorkerProcess;
using ray::core::RayFunction;
using ray::core::TaskOptions;

struct RustTaskArg;

using TaskArgs = std::unique_ptr<::ray::TaskArg>;

std::vector<std::unique_ptr<::ray::TaskArg>> TransformArgs(
    const rust::Vec<RustTaskArg> &args);

std::unique_ptr<ObjectID> Submit(rust::Str name, const rust::Vec<RustTaskArg> &args);

void InitRust(rust::Str str);

Status ExecuteTask(
    ray::TaskType task_type, const std::string task_name, const RayFunction &ray_function,
    const std::unordered_map<std::string, double> &required_resources,
    const std::vector<std::shared_ptr<ray::RayObject>> &args_buffer,
    const std::vector<rpc::ObjectReference> &arg_refs,
    const std::vector<ObjectID> &return_ids, const std::string &debugger_breakpoint,
    std::vector<std::shared_ptr<ray::RayObject>> *results,
    std::shared_ptr<ray::LocalMemoryBuffer> &creation_task_exception_pb_bytes,
    bool *is_application_level_error,
    const std::vector<ConcurrencyGroup> &defined_concurrency_groups,
    const std::string name_of_concurrency_group_to_execute);
}

rust::Vec<uint8_t> GetRaw(std::unique_ptr<ObjectID> id);

std::unique_ptr<ObjectID> PutRaw(rust::Vec<uint8_t> data);

void LogDebug(rust::Str str);

void LogInfo(rust::Str str);

std::unique_ptr<std::string> ObjectIDString(std::unique_ptr<ObjectID> id);

std::unique_ptr<ObjectID> StringObjectID(const std::string &string);'

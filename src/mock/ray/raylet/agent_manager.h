namespace ray {
namespace raylet {

class MockAgentManager : public AgentManager {
 public:
  MOCK_METHOD(void, HandleRegisterAgent, (const rpc::RegisterAgentRequest &request, rpc::RegisterAgentReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
  MOCK_METHOD(void, CreateRuntimeEnv, (const JobID &job_id, const std::string &serialized_runtime_env, CreateRuntimeEnvCallback callback), (override));
  MOCK_METHOD(void, DeleteRuntimeEnv, (const std::string &serialized_runtime_env, DeleteRuntimeEnvCallback callback), (override));
};

}  // namespace raylet
}  // namespace ray

namespace ray {
namespace raylet {

class MockDefaultAgentManagerServiceHandler : public DefaultAgentManagerServiceHandler {
 public:
  MOCK_METHOD(void, HandleRegisterAgent, (const rpc::RegisterAgentRequest &request, rpc::RegisterAgentReply *reply, rpc::SendReplyCallback send_reply_callback), (override));
};

}  // namespace raylet
}  // namespace ray

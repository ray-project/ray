extern "C" {

void* orch_create_context(const char* server_addr);
size_t orch_remote_call(void* context, const char* name, void* args);

}

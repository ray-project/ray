extern "C" {

void* orch_create_context(const char* server_addr);
size_t orch_remote_call(void* context, const char* name, void* args);

void* orch_arglist_create();
void orch_arglist_add_ref(void* arglist, size_t ref);
void orch_arglist_add_string(void* arglist, const char* str);
void orch_arglist_destroy(void* arglist);

}

#include <dlfcn.h>
#include <mach-o/dyld.h>
#include <mach-o/nlist.h>
#include <stdio.h>
#include <string.h>
#include <memory>
#include <msgpack.hpp>
#include "function_helper.h"
#include "ray/core.h"
#include "address_helper.h"

namespace ray {
namespace api {

#ifdef __LP64__
typedef struct mach_header_64 mach_header_t;
typedef struct segment_command_64 segment_command_t;
typedef struct nlist_64 nlist_t;
#else
typedef struct mach_header mach_header_t;
typedef struct segment_command segment_command_t;
typedef struct nlist nlist_t;
#endif

static const char * first_external_symbol_for_image(const mach_header_t *header)
{
    Dl_info info;
    if (dladdr(header, &info) == 0)
        return NULL;

    segment_command_t *seg_linkedit = NULL;
    segment_command_t *seg_text = NULL;
    struct symtab_command *symtab = NULL;

    struct load_command *cmd = (struct load_command *)((intptr_t)header + sizeof(mach_header_t));
    for (uint32_t i = 0; i < header->ncmds; i++, cmd = (struct load_command *)((intptr_t)cmd + cmd->cmdsize))
    {
        switch(cmd->cmd)
        {
            case LC_SEGMENT:
            case LC_SEGMENT_64:
                if (!strcmp(((segment_command_t *)cmd)->segname, SEG_TEXT))
                    seg_text = (segment_command_t *)cmd;
                else if (!strcmp(((segment_command_t *)cmd)->segname, SEG_LINKEDIT))
                    seg_linkedit = (segment_command_t *)cmd;
                break;

            case LC_SYMTAB:
                symtab = (struct symtab_command *)cmd;
                break;
        }
    }

    if ((seg_text == NULL) || (seg_linkedit == NULL) || (symtab == NULL))
        return NULL;

    intptr_t file_slide = ((intptr_t)seg_linkedit->vmaddr - (intptr_t)seg_text->vmaddr) - seg_linkedit->fileoff;
    intptr_t strings = (intptr_t)header + (symtab->stroff + file_slide);
    nlist_t *sym = (nlist_t *)((intptr_t)header + (symtab->symoff + file_slide));

    for (uint32_t i = 0; i < symtab->nsyms; i++, sym++)
    {
        if ((sym->n_type & N_EXT) != N_EXT || !sym->n_value)
            continue;

        return (const char *)strings + sym->n_un.n_strx;
    }

    return NULL;
}

uintptr_t base_addr = 0;


static const uintptr_t BaseAddressForHandle(void *handle)
{
    for (int32_t i = _dyld_image_count(); i >= 0 ; i--)
    {
        const char *first_symbol = first_external_symbol_for_image((const mach_header_t *)_dyld_get_image_header(i));
        if (first_symbol && strlen(first_symbol) > 1)
        {
            handle = (void *)((intptr_t)handle | 1); 
            first_symbol++;
            void *address = dlsym(handle, first_symbol);
            Dl_info info;
            if (dladdr(address, &info)){
                base_addr = (uintptr_t)info.dli_fbase;
                return base_addr;
            }
        }
    }
    return -1;
}

uintptr_t FunctionHelper::LoadLibrary(std::string lib_name) {
    if (dynamic_library_base_addr != 0) {
        return dynamic_library_base_addr;
    }
    RAY_LOG(INFO) << "Start load library " << lib_name;
    void *example = dlopen(lib_name.c_str(), RTLD_LAZY);
    uintptr_t base_addr = BaseAddressForHandle(example);
    RAY_CHECK(base_addr > 0);
    RAY_LOG(INFO) << "Load library " << lib_name << " to base address " << base_addr;
    loaded_library_.emplace(lib_name, base_addr);
    return base_addr;
}

uintptr_t FunctionHelper::GetBaseAddress(std::string lib_name) {
    auto got = loaded_library_.find(lib_name);
    if (got == loaded_library_.end()) {
        return LoadLibrary(lib_name);
    }
    return got->second;
}

std::shared_ptr<FunctionHelper> FunctionHelper::function_helper_ = nullptr;

std::shared_ptr<FunctionHelper> FunctionHelper::GetInstance() {
  if (function_helper_ == nullptr) {
    function_helper_ = std::make_shared<FunctionHelper>();
  }
  return function_helper_;
}
}  // namespace api
}  // namespace ray
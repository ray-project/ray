

// #include <stdio.h>
// #include <stdlib.h>
// #include <dlfcn.h>

// int main() {
//     struct link_map *lm = (struct link_map*) dlopen("example.so", RTLD_NOW);
//     printf("%p\n", lm->l_addr);
// }

#include <dlfcn.h>
#include <mach-o/dyld.h>
#include <mach-o/nlist.h>
#include <stdio.h>
#include <string.h>
#include <memory>
#include <msgpack.hpp>

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


const char * pathname_for_handle(void *handle)
{
    for (int32_t i = _dyld_image_count(); i >= 0 ; i--)
    {
        const char *first_symbol = first_external_symbol_for_image((const mach_header_t *)_dyld_get_image_header(i));
        if (first_symbol && strlen(first_symbol) > 1)
        {
            handle = (void *)((intptr_t)handle | 1); // in order to trigger findExportedSymbol instead of findExportedSymbolInImageOrDependentImages. See `dlsym` implementation at http://opensource.apple.com/source/dyld/dyld-239.3/src/dyldAPIs.cpp
            first_symbol++; // in order to remove the leading underscore
            void *address = dlsym(handle, first_symbol);
            Dl_info info;
            if (dladdr(address, &info)){
                printf("dli_fbase %ld\n", (uintptr_t)info.dli_fbase);
                base_addr = (uintptr_t)info.dli_fbase;
                return info.dli_fname;
            }
        }
    }
    return NULL;
}

int main(int argc, const char * argv[])
{
    void *example = dlopen("/Users/jiulong/arcos/opensource/ant/ray/bazel-bin/cpp/example.so", RTLD_LAZY);
    printf("example path: %s\n", pathname_for_handle(example));

    typedef std::shared_ptr<msgpack::sbuffer> (*ExecFunction)(
    uintptr_t base_addr, size_t func_offset, std::shared_ptr<msgpack::sbuffer> args,
    std::shared_ptr<msgpack::sbuffer> object);
    unsigned long offset = 285872;
    auto address = base_addr + offset;
    ExecFunction exec_function = (ExecFunction)(
        address);
    printf("function %s\n", (char *)exec_function);
    dlclose(example);
    return 0;
}

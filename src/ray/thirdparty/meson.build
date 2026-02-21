# === @begin: sha256 (original: sha256) ===
sha256_cpp_lib = static_library(
    'sha256_cpp_lib',
    ['sha256.c']
)

sha256_cpp_lib_dep = declare_dependency(
    link_with: sha256_cpp_lib
)
# === @end: sha256 (original: sha256) ===


# === @begin: aligned_alloc (original: aligned_alloc) ===
aligned_alloc_cpp_lib = static_library(
    'aligned_alloc_cpp_lib',
    ['aligned_alloc.c']
)

aligned_alloc_cpp_lib_dep = declare_dependency(
    link_with: aligned_alloc_cpp_lib
)
# === @end: aligned_alloc (original: aligned_alloc) ===



# === @begin: dlmalloc (original: dlmalloc) ===
# Header-only: dlmalloc.c is included via #include in upstream code
# dlmalloc.c is a textual include — it uses the "unity build" pattern where the entire C file is #included into a wrapper translation unit. Compiling it as a separate static library would produce duplicate symbol definitions when linked together with plasma/dlmalloc.cc, causing a linker error
dlmalloc_cpp_lib_dep = declare_dependency(
    sources: ['dlmalloc.c']
)
# === @end: dlmalloc (original: dlmalloc) ===
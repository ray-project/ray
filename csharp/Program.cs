using System.Runtime.InteropServices;
// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

// need to set e.g.
// LD_LIBRARY_PATH=~/Desktop/Programming/systems/ray/python/ray/rust/lib:LD_LIBRARY_PATH

[DllImport("libcore_worker_library_c")]
static extern void c_worker_InitConfig(
  int worker_type, int language, int num_workers,
  String code_path, String head_args,
  int argc, String[] argv
);
[DllImport("libcore_worker_library_c")]
static extern void c_worker_Initialize();
[DllImport("libcore_worker_library_c")]
static extern void c_worker_Shutdown();
[DllImport("libcore_worker_library_c")]
static extern void c_worker_RegisterExecutionCallback(
  
);
//
// // C# callbacks: https://stackoverflow.com/questions/10841081/c-sharp-c-interop-callback
// [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
// CSharpWorkerExecute();
//
// c_worker_RegisterExecutionCallback(CSharpWorkerExecute);


String[] empty = new String[1] { "" };
c_worker_InitConfig(1, 3, 1, "", "", 0, empty);
Console.WriteLine("Initialized Config");
c_worker_Initialize();
Console.WriteLine("Shutting down...");
c_worker_Shutdown();

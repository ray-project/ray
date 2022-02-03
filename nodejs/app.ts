var ffi = require('ffi-napi');

var cw = ffi.Library('libcore_worker_library_c', {
  'c_worker_InitConfig': [ 'void', ['int32', 'int32', 'int32', 'string', 'string', 'int32', 'pointer'] ],
  'c_worker_Initialize': [ 'void', [] ],
  'c_worker_Shutdown': [ 'void', [] ],
});

cw.c_worker_InitConfig(1, 3, 1, "", "", 0, null)
cw.c_worker_Initialize();
cw.c_worker_Shutdown();

// TODO: create node.js C callback with stable address

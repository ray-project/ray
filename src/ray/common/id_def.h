// This header file is used to avoid code duplication.
// It can be included multiple times in id.h, and each inclusion
// could use a different definition of the DEFINE_UNIQUE_ID macro.
// Macro definition format: DEFINE_UNIQUE_ID(id_type).
// NOTE: This file should NOT be included in any file other than id.h.

DEFINE_UNIQUE_ID(FunctionID)
DEFINE_UNIQUE_ID(ActorClassID)
DEFINE_UNIQUE_ID(ActorHandleID)
DEFINE_UNIQUE_ID(ActorCheckpointID)
DEFINE_UNIQUE_ID(WorkerID)
DEFINE_UNIQUE_ID(ConfigID)
DEFINE_UNIQUE_ID(ClientID)

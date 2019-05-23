// This header file is used to avoid code duplication.
// It can be included multiple times in id.h, and each inclusion
// could use a different definition of the DEFINE_UNIQUE_ID macro.
// Macro definition format: DEFINE_UNIQUE_ID(id_type).
// NOTE: This file should NOT be included in any file other than id.h.

DEFINE_UNIQUE_ID(FunctionId)
DEFINE_UNIQUE_ID(ActorClassId)
DEFINE_UNIQUE_ID(ActorId)
DEFINE_UNIQUE_ID(ActorHandleId)
DEFINE_UNIQUE_ID(ActorCheckpointId)
DEFINE_UNIQUE_ID(WorkerId)
DEFINE_UNIQUE_ID(DriverId)
DEFINE_UNIQUE_ID(ConfigId)
DEFINE_UNIQUE_ID(ClientId)

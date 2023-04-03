// Copyright 2020-2023 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const ACTOR_ID_UNIQUE_BYTES: usize = 12;
const TASK_ID_UNIQUE_BYTES: usize = 8;

pub const UNIQUE_ID_SIZE: usize = 28;
pub const JOB_ID_SIZE: usize = 4;

pub const ACTOR_ID_SIZE: usize = JOB_ID_SIZE + ACTOR_ID_UNIQUE_BYTES;
pub const TASK_ID_SIZE: usize = ACTOR_ID_SIZE + TASK_ID_UNIQUE_BYTES;
const MAX_OBJECT_ID_INDEX: u32 = ((1 as u64) << 32 - 1) as u32;
pub const OBJECT_ID_SIZE: usize = TASK_ID_SIZE + 4;

pub const PLACEMENT_GROUP_ID_SIZE: usize = JOB_ID_SIZE + 14;

pub type FunctionID = UniqueID;
pub type ActorClassID = UniqueID;
pub type WorkerID = UniqueID;
pub type ConfigID = UniqueID;
pub type NodeID = UniqueID;

pub trait Base<T> {
    fn size(&self) -> usize;
    fn from_random() -> T;
    fn from_binary(data: &[u8]) -> T;
    fn from_hex_string(hex: &str) -> T;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObjectID {
    pub id: [u8; OBJECT_ID_SIZE],
}

impl ObjectID {
    pub fn new() -> ObjectID {
        ObjectID {
            id: [0; OBJECT_ID_SIZE],
        }
    }
}

impl Base<ObjectID> for ObjectID {
    fn size(&self) -> usize {
        OBJECT_ID_SIZE
    }

    fn from_random() -> ObjectID {
        unimplemented!()
    }

    fn from_binary(data: &[u8]) -> ObjectID {
        unimplemented!()
    }

    fn from_hex_string(hex: &str) -> ObjectID {
        unimplemented!()
    }
}

pub struct UniqueID {
    pub id: [u8; UNIQUE_ID_SIZE],
}

impl UniqueID {
    pub fn new() -> UniqueID {
        UniqueID {
            id: [0; UNIQUE_ID_SIZE],
        }
    }
}

impl Base<UniqueID> for UniqueID {
    fn size(&self) -> usize {
        UNIQUE_ID_SIZE
    }

    fn from_random() -> UniqueID {
        unimplemented!()
    }

    fn from_binary(data: &[u8]) -> UniqueID {
        unimplemented!()
    }

    fn from_hex_string(hex: &str) -> UniqueID {
        unimplemented!()
    }
}

pub struct ActorID {
    pub id: [u8; ACTOR_ID_SIZE],
}

impl ActorID {
    pub fn new() -> ActorID {
        ActorID {
            id: [0; ACTOR_ID_SIZE],
        }
    }
}

impl Base<ActorID> for ActorID {
    fn size(&self) -> usize {
        ACTOR_ID_SIZE
    }

    fn from_random() -> ActorID {
        unimplemented!()
    }

    fn from_binary(data: &[u8]) -> ActorID {
        unimplemented!()
    }

    fn from_hex_string(hex: &str) -> ActorID {
        unimplemented!()
    }
}

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
use serde;
use serde_json;
use std::collections::HashMap;

mod core;
mod helper;
mod hostcalls;
mod id;
mod obj_store;
mod runtime;
mod task_executor;
mod task_submitter;

pub use crate::runtime::core::*;
pub use crate::runtime::helper::*;
pub use crate::runtime::hostcalls::*;
pub use crate::runtime::id::*;
pub use crate::runtime::obj_store::*;
pub use crate::runtime::runtime::*;
pub use crate::runtime::task_executor::*;
pub use crate::runtime::task_submitter::*;

#[derive(PartialEq)]
pub enum ActorLifetime {
    Detached,
    NonDetached,
}

#[derive(Clone, Debug)]
pub struct RuntimeEnv(HashMap<String, serde_json::Value>);

pub mod common_proto {
    include!(concat!(env!("OUT_DIR"), "/ray.rpc.rs"));
}

impl RuntimeEnv {
    pub fn set<T: serde::ser::Serialize>(&mut self, key: &str, value: T) {
        self.0
            .insert(key.to_string(), serde_json::to_value(value).unwrap());
    }

    pub fn get<T: for<'de> serde::Deserialize<'de> + Clone>(&self, key: &str) -> Option<T> {
        self.0.get(key).and_then(|v| v.as_str()).and_then(|v| {
            let val: T = serde_json::from_str::<T>(v).unwrap().clone();
            Some(val)
        })
    }

    // static function to serialize RuntimeEnv
    pub fn serialize_to_json(re: &RuntimeEnv) -> String {
        serde_json::to_string(&re.0).unwrap()
    }

    // static function to deserialize RuntimeEnv
    pub fn deserialize_from_json_string(data: &str) -> RuntimeEnv {
        let tmp = serde_json::from_str(data).unwrap();
        RuntimeEnv(tmp)
    }

    pub fn deserialize_from_json_value(data: serde_json::Value) -> RuntimeEnv {
        let tmp = data.as_object().unwrap().clone();
        let mut res = HashMap::new();
        for (k, v) in tmp.iter() {
            res.insert(k.to_string(), v.clone());
        }
        RuntimeEnv(res)
    }
}

pub struct RayConfig {
    // The address of the Ray cluster to connect to.
    // If not provided, it will be initialized from environment variable "RAY_ADDRESS" by
    // default.
    pub address: String,

    // Whether or not to run this application in a local mode. This is used for debugging.
    pub local_mode: bool,

    // An array of directories or dynamic library files that specify the search path for
    // user code. This parameter is not used when the application runs in local mode.
    // Only searching the top level under a directory.
    pub code_search_path: Vec<String>,

    // The command line args to be appended as parameters of the `ray start` command. It
    // takes effect only if Ray head is started by a driver. Run `ray start --help` for
    // details.
    pub head_args: Vec<String>,

    // The default actor lifetime type, `DETACHED` or `NON_DETACHED`.
    pub default_actor_lifetime: ActorLifetime,

    // The job level runtime environments.
    pub runtime_env: Option<RuntimeEnv>,

    /* The following are unstable parameters and their use is discouraged. */
    // Prevents external clients without the password from connecting to Redis if provided.
    pub redis_password: Option<String>,

    // A specific flag for internal `default_worker`. Please don't use it in user code.
    pub is_worker: bool,

    // A namespace is a logical grouping of jobs and named actors.
    pub ray_namespace: String,
}

impl RayConfig {
    pub fn new() -> Self {
        Self {
            address: "".to_string(),
            local_mode: false,
            code_search_path: vec![],
            head_args: vec![],
            default_actor_lifetime: ActorLifetime::NonDetached,
            runtime_env: None,
            redis_password: None,
            is_worker: false,
            ray_namespace: "".to_string(),
        }
    }
}

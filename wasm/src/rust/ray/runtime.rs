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

use crate::ray::ClusterHelper;
use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::info;

pub struct RayRuntime {}

lazy_static! {
    /// Runtime implementation instance.
    static ref RAY_RUNTIME: Arc<Mutex<RayRuntime>> = {
        let runtime = RayRuntime {};
        Arc::new(Mutex::new(runtime))
    };
}

impl RayRuntime {
    pub fn do_init(&self) {
        let cfg_w_lcok = crate::config::ConfigInternal::instance();
        match cfg_w_lcok.lock().unwrap().run_mode {
            crate::config::RunMode::SingleProcess => {
                // TODO: start local mode
            }
            crate::config::RunMode::Cluster => {
                info!("native ray runtime started.");
                // TODO: start cluster mode
            }
        };

        //TODO: fix me. this is only for testing
        ClusterHelper::instance().lock().unwrap().do_init();

        ClusterHelper::instance().lock().unwrap().ray_start();
        // ClusterHelper::instance().lock().unwrap().ray_node_start(
        //     "127.0.0.1",
        //     1234,
        //     "123456",
        //     &vec![] as &Vec<String>,
        // );
        ClusterHelper::instance().lock().unwrap().ray_node_stop();
    }

    /// Return a mutex protected instance of the runtime.
    pub fn instance() -> Arc<Mutex<RayRuntime>> {
        RAY_RUNTIME.clone()
    }

    pub fn do_shutdown(&self) {
        // check if we are running in cluster mode
        if crate::config::ConfigInternal::instance()
            .lock()
            .unwrap()
            .run_mode
            == crate::config::RunMode::Cluster
        {
            // TODO: terminate cluster mode
        }
    }
}

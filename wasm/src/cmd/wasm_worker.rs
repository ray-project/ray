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
use wasm_on_ray::config;
use wasm_on_ray::runtime;
use wasm_on_ray::runtime::RayRuntime;
use wasm_on_ray::util;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber;

async fn init(
    cfg: &runtime::RayConfig,
    args: &util::WorkerParameters,
) -> Result<Box<dyn RayRuntime + Send + Sync>> {
    let mut internal_cfg = config::ConfigInternal::new();

    internal_cfg.init(&cfg, &args);

    let mut runtime = runtime::RayRuntimeFactory::create_runtime(internal_cfg).unwrap();
    runtime.do_init().unwrap();

    Ok(runtime)
}

async fn run_task_loop(runtime: Box<dyn RayRuntime + Send + Sync>) -> Result<()> {
    runtime.launch_task_loop().unwrap();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let args = util::WorkerParameters::parse();
    let mut cfg = runtime::RayConfig::new();
    
    // we need to run in worker mode
    cfg.is_worker = true;
 
    let rt = init(&cfg, &args).await.unwrap();
    run_task_loop(rt).await.unwrap();

    Ok(())
}

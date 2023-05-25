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
use wasm_on_ray::engine::{WasmEngine, WasmEngineFactory, WasmEngineType};
use wasm_on_ray::runtime::common_proto::WorkerType;
use wasm_on_ray::runtime::{
    register_ray_hostcalls, ClusterHelper, RayConfig, RayRuntime, RayRuntimeFactory,
};
use wasm_on_ray::util::RayLog;
use wasm_on_ray::util::{WasmEngineTypeParam, WorkerParameters};

use std::sync::{Arc, RwLock};
use tracing::error;

use anyhow::{anyhow, Result};
use clap::Parser;
use tracing_subscriber;

struct WorkerContext {
    // ray runtime
    runtime: Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,

    // wasm engine
    engine: Arc<RwLock<Box<dyn WasmEngine + Send + Sync>>>,
}

async fn init_runtime(
    cfg: &RayConfig,
    args: &WorkerParameters,
) -> Result<Box<dyn RayRuntime + Send + Sync>> {
    let mut internal_cfg = config::ConfigInternal::new();

    internal_cfg.init(&cfg, &args);

    let mut runtime = RayRuntimeFactory::create_runtime(internal_cfg).unwrap();
    runtime.do_init().unwrap();

    Ok(runtime)
}

async fn init_engine(args: &WorkerParameters) -> Result<Box<dyn WasmEngine + Send + Sync>> {
    let engine_type = match args.engine_type {
        WasmEngineTypeParam::WASMEDGE => WasmEngineType::WASMEDGE,
        WasmEngineTypeParam::WASMTIME => WasmEngineType::WASMTIME,
        _ => unimplemented!(),
    };
    let engine = WasmEngineFactory::create_engine(engine_type).unwrap();
    engine.init().unwrap();

    Ok(engine)
}

async fn run_task_loop(ctx: &mut WorkerContext) -> Result<()> {
    let runtime = &ctx.runtime;
    let engine = &ctx.engine;
    match runtime.write().unwrap().spawn_task_loop() {
        Ok(_) => {}
        Err(e) => {
            error!("spawn task loop failed: {:?}", e);
            return Err(anyhow!("spawn task loop failed: {:?}", e));
        }
    }
    loop {
        if !runtime.write().unwrap().is_running() {
            break;
        }
        match engine.write().unwrap().task_loop_once(runtime) {
            Ok(_) => {}
            Err(e) => {
                error!("task loop once failed: {:?}", e);
                return Err(anyhow!("task loop once failed: {:?}", e));
            }
        }
    }
    Ok(())
}

fn init_wasm_module(
    runtime: &Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,
    engine: &Arc<RwLock<Box<dyn WasmEngine + Send + Sync>>>,
) -> Result<()> {
    let rt = runtime.write().unwrap();
    let mut _engine = engine.write().unwrap();
    if rt.exec_type() == WorkerType::Worker {
        let _modules = ClusterHelper::wasm_modules();
        // TODO: process wasm modules found in the search paths.
    }
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

    let args = WorkerParameters::parse();
    let mut cfg = RayConfig::new();

    // we need to run in worker mode
    cfg.is_worker = true;

    let rt = init_runtime(&cfg, &args).await.unwrap();
    let engine = init_engine(&args).await.unwrap();

    let mut ctx = WorkerContext {
        runtime: Arc::new(RwLock::new(rt)),
        engine: Arc::new(RwLock::new(engine)),
    };

    RayLog::info("register ray hostcalls");
    // setup hostcalls
    register_ray_hostcalls(&ctx.runtime, &ctx.engine).unwrap();

    init_wasm_module(&ctx.runtime, &ctx.engine).unwrap();

    run_task_loop(&mut ctx).await.unwrap();

    Ok(())
}

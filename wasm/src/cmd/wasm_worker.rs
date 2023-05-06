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
use wasm_on_ray::util::{self, RayLog};

use std::sync::{Arc, RwLock};
use tracing::error;

use anyhow::{anyhow, Result};
use clap::Parser;
use sha256::digest;
use tracing_subscriber;

struct WorkerContext {
    // ray runtime
    runtime: Arc<RwLock<Box<dyn RayRuntime + Send + Sync>>>,

    // wasm engine
    engine: Arc<RwLock<Box<dyn WasmEngine + Send + Sync>>>,
}

async fn init_runtime(
    cfg: &RayConfig,
    args: &util::WorkerParameters,
) -> Result<Box<dyn RayRuntime + Send + Sync>> {
    let mut internal_cfg = config::ConfigInternal::new();

    internal_cfg.init(&cfg, &args);

    let mut runtime = RayRuntimeFactory::create_runtime(internal_cfg).unwrap();
    runtime.do_init().unwrap();

    Ok(runtime)
}

async fn init_engine(args: &util::WorkerParameters) -> Result<Box<dyn WasmEngine + Send + Sync>> {
    let engine_type = match args.engine_type {
        util::WasmEngineType::Wasmedge => WasmEngineType::WASMEDGE,
        util::WasmEngineType::Wasmtime => WasmEngineType::WASMTIME,
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
        match engine.write().unwrap().task_loop_once() {
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
    let mut rt = runtime.write().unwrap();
    let mut engine = engine.write().unwrap();
    if rt.exec_type() == WorkerType::Worker {
        let modules = ClusterHelper::wasm_modules();
        // TODO: process cases of more than one wasm modules
        match modules.len() {
            0 => {
                RayLog::error(
                    "worker mode need at least one wasm module"
                        .to_string()
                        .as_str(),
                );
                return Err(anyhow!("worker mode need at least one wasm module"));
            }
            1 => {
                let wasm_file = std::path::Path::new(modules[0].as_str());
                let wasm_bytes = match std::fs::read(wasm_file) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        RayLog::error(format!("read wasm file failed: {:?}", e).as_str());
                        return Err(anyhow!("read wasm file failed: {:?}", e));
                    }
                };
                // calculate the sha256 hash of wasm bytes
                let wasm_bytes_hash = digest(wasm_bytes.as_slice());
                match engine.compile("module", &wasm_bytes) {
                    Ok(_) => {}
                    Err(e) => {
                        RayLog::error(format!("compile wasm module failed: {:?}", e).as_str());
                        return Err(anyhow!("compile wasm module failed: {:?}", e));
                    }
                };
                match engine.create_sandbox("sandbox") {
                    Ok(_) => {}
                    Err(e) => {
                        RayLog::error(format!("create wasm sandbox failed: {:?}", e).as_str());
                        return Err(anyhow!("create wasm sandbox failed: {:?}", e));
                    }
                };
                match engine.instantiate("sandbox", "module", "instance") {
                    Ok(_) => {}
                    Err(e) => {
                        RayLog::error(format!("instantiate wasm module failed: {:?}", e).as_str());
                        return Err(anyhow!("instantiate wasm module failed: {:?}", e));
                    }
                };
            }
            _ => {
                RayLog::error(
                    "worker mode only support one wasm module"
                        .to_string()
                        .as_str(),
                );
                return Err(anyhow!("worker mode only support one wasm module"));
            }
        }
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

    let args = util::WorkerParameters::parse();
    let mut cfg = RayConfig::new();

    // we need to run in worker mode
    cfg.is_worker = true;

    let rt = init_runtime(&cfg, &args).await.unwrap();
    let mut engine = init_engine(&args).await.unwrap();

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

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
use wasm_on_ray::engine;
use wasm_on_ray::runtime;
use wasm_on_ray::runtime::register_ray_hostcalls;
use wasm_on_ray::util::{
    LauncherParameters, WasmEngineTypeParam, WasmFileFormat, WorkerParameters,
};

use tracing::{debug, error};

use anyhow::{anyhow, Result};
use clap::Parser;
use std::sync::{Arc, RwLock};
use tracing_subscriber;

struct RayWaContext {
    // ray runtime
    runtime: Arc<RwLock<Box<dyn runtime::RayRuntime + Send + Sync>>>,

    // wasm engine
    engine: Arc<RwLock<Box<dyn engine::WasmEngine + Send + Sync>>>,

    // launch parameters
    params: LauncherParameters,

    // ray internal config
    config: config::ConfigInternal,
}

struct RayWaContextFactory {}

impl RayWaContextFactory {
    pub async fn create_context(params: &LauncherParameters) -> Result<RayWaContext> {
        let cfg = runtime::RayConfig::new();
        let mut internal_cfg = config::ConfigInternal::new();

        internal_cfg.init(&cfg, &WorkerParameters::new_empty());

        let wasm_engine = RayWaContextFactory::create_engine(params.engine_type.clone());
        let ray_runtime = RayWaContextFactory::create_runtime(internal_cfg.clone());
        let (wasm_engine, ray_runtime) = tokio::join!(wasm_engine, ray_runtime);

        let context = RayWaContext {
            runtime: Arc::new(RwLock::new(ray_runtime.unwrap())),
            engine: Arc::new(RwLock::new(wasm_engine.unwrap())),
            params: params.clone(),
            config: internal_cfg,
        };

        // init runtime and engine
        {
            let mut rt = context.runtime.write().unwrap();
            rt.do_init()?;

            let engine = context.engine.write().unwrap();
            engine.init()?;
        }

        // setup hostcalls
        register_ray_hostcalls(&context.runtime, &context.engine)?;

        Ok(context)
    }

    async fn create_engine(
        engine_type: WasmEngineTypeParam,
    ) -> Result<Box<dyn engine::WasmEngine + Send + Sync>> {
        let engine_type = match engine_type {
            WasmEngineTypeParam::WASMEDGE => engine::WasmEngineType::WASMEDGE,
            WasmEngineTypeParam::WASMTIME => engine::WasmEngineType::WASMTIME,
            _ => unimplemented!(),
        };
        let e = engine::WasmEngineFactory::create_engine(engine_type);
        Ok(e.unwrap())
    }

    async fn create_runtime(
        internal_cfg: config::ConfigInternal,
    ) -> Result<Box<dyn runtime::RayRuntime + Send + Sync>> {
        let runtime = runtime::RayRuntimeFactory::create_runtime(internal_cfg);
        Ok(runtime.unwrap())
    }
}

async fn run_binary(args: &LauncherParameters) -> Result<()> {
    let ctx = RayWaContextFactory::create_context(&args);

    // check if wasm file exists
    let wasm_file = std::path::Path::new(&args.file);
    if !wasm_file.exists() {
        return Err(anyhow!("wasm file \"{}\" not found", wasm_file.display()));
    }
    // if file found, read it and compile it
    let data = std::fs::read(wasm_file)?;

    // wait for context creation to finish
    let ctx = ctx.await?;

    match ctx.engine.write() {
        Ok(mut engine) => {
            let _module = engine.compile("module", &data)?;
            let _sandbox = engine.create_sandbox("sandbox")?;
            let _instance = engine.instantiate("sandbox", "module", "instance")?;

            debug!("wasm module instantiated");

            match engine.execute("sandbox", "instance", "_start", vec![]) {
                Ok(_) => debug!("wasm module completed execution"),
                Err(e) => error!("wasm module execution failed: {}", e),
            }
        }
        Err(e) => error!("failed to acquire engine lock: {}", e),
    }

    // for now, we just shutdown the runtime
    match ctx.runtime.write() {
        Ok(mut rt) => {
            rt.do_shutdown()?;
        }
        Err(e) => error!("failed to acquire runtime lock: {}", e),
    }

    Ok(())
}

async fn run_text(_args: &LauncherParameters) -> Result<()> {
    unimplemented!()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let args = LauncherParameters::parse();

    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    // load data
    match args.file_format {
        WasmFileFormat::WASM => run_binary(&args).await?,
        WasmFileFormat::WAT => run_text(&args).await?,
    }

    Ok(())
}

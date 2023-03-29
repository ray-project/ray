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
use wasm_on_ray::engine::WasmInstance;
use wasm_on_ray::ray;
use wasm_on_ray::util;

use std::sync::{Arc, RwLock};
use tokio::task;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber;

struct RayWaContext {
    // ray runtime
    runtime: Box<dyn ray::RayRuntime>,

    // wasm engine
    engine: Box<dyn engine::WasmEngine>,

    // launch parameters
    params: util::LauncherParameters,

    // ray internal config
    config: config::ConfigInternal,
}

struct RayWaContextFactory {}

impl RayWaContextFactory {
    pub async fn create_context(params: util::LauncherParameters) -> Result<RayWaContext> {
        let cfg = ray::RayConfig::new();
        let mut internal_cfg = config::ConfigInternal::new();

        internal_cfg.init(&cfg, &util::WorkerParameters::new_empty());

        let wasm_engine = RayWaContextFactory::create_engine(params.engine_type.clone());
        let ray_runtime = RayWaContextFactory::create_runtime(internal_cfg.clone());
        let (wasm_engine, ray_runtime) = tokio::join!(wasm_engine, ray_runtime);

        let mut context = RayWaContext {
            runtime: ray_runtime.unwrap(),
            engine: wasm_engine.unwrap(),
            params,
            config: internal_cfg,
        };

        context.runtime.do_init()?;
        Ok(context)
    }

    async fn create_engine(
        engine_type: util::WasmEngineType,
    ) -> Result<Box<dyn engine::WasmEngine>> {
        let engine_type = match engine_type {
            util::WasmEngineType::Wasmedge => engine::WasmEngineType::WASMEDGE,
            util::WasmEngineType::Wasmtime => engine::WasmEngineType::WASMTIME,
            _ => unimplemented!(),
        };
        let e = engine::WasmEngineFactory::create_engine(engine_type);
        Ok(e)
    }

    async fn create_runtime(
        internal_cfg: config::ConfigInternal,
    ) -> Result<Box<dyn ray::RayRuntime>> {
        let runtime = ray::RayRuntimeFactory::create_runtime(internal_cfg);
        Ok(runtime.unwrap())
    }
}

// lazy_static::lazy_static! {
//     static ref ENGINE_INSTANCE: Arc<RwLock<Option<Box<dyn engine::WasmEngine>>>> = Arc::new(RwLock::new(None));
// }

// async fn init_wasm(args: &util::LauncherParameters) -> Result<Box<dyn WasmInstance>> {
//     let engine_type = match args.engine_type {
//         util::WasmEngineType::Wasmedge => engine::WasmEngineType::WASMEDGE,
//         util::WasmEngineType::Wasmtime => engine::WasmEngineType::WASMTIME,
//         _ => unimplemented!(),
//     };

//     {
//         let engine_instance = engine::WasmEngineFactory::create_engine(engine_type);
//         ENGINE_INSTANCE.write().unwrap().replace(engine_instance);
//     }

//     let mut hostcalls = ray::Hostcalls::new();
//     ENGINE_INSTANCE
//         .read()
//         .unwrap()
//         .as_ref()
//         .unwrap()
//         .register_hostcalls(&mut hostcalls)?;

//     // check if wasm file exists
//     let wasm_file = std::path::Path::new(&args.wasm_file);
//     if !wasm_file.exists() {
//         return Err(anyhow::anyhow!(
//             "wasm file \"{}\" not found",
//             wasm_file.display()
//         ));
//     }
//     // if file found, read it and compile it
//     let data = std::fs::read(wasm_file)?;

//     let engine_instance = ENGINE_INSTANCE.read().unwrap();
//     // compile wasm file
//     let wasm_module = engine_instance.as_ref().unwrap().compile(&data)?;

//     // crate a new instance
//     let sandbox = engine_instance.as_ref().unwrap().create_sandbox()?;
//     let wasm_instance = engine_instance
//         .as_ref()
//         .unwrap()
//         .instantiate(sandbox, wasm_module)?;

//     Ok(wasm_instance)
// }

// async fn init_ray(args: &util::LauncherParameters) -> Result<()> {
//     let cfg = ray::RayConfig::new();
//     let internal_cfg = config::ConfigInternal::new();

//     internal_cfg.init(&cfg, &util::WorkerParameters::new_empty());
//     {
//         ray::RayRuntime::instance()
//             .write()
//             .unwrap()
//             .do_init()
//             .await
//             .unwrap();
//     }
//     Ok(())
// }

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let args = util::LauncherParameters::parse();

    // let wasm_instance = init_wasm(&args);
    // let ray_instance = init_ray(&args);

    // let (wasm_instance, ray_instance) = tokio::join!(wasm_instance, ray_instance);
    let ctx = RayWaContextFactory::create_context(args).await?;

    Ok(())
}

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
use wasm_on_ray::ray;
use wasm_on_ray::util;

use tracing::info;

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
    pub async fn create_context(params: &util::LauncherParameters) -> Result<RayWaContext> {
        let cfg = ray::RayConfig::new();
        let mut internal_cfg = config::ConfigInternal::new();

        internal_cfg.init(&cfg, &util::WorkerParameters::new_empty());

        let wasm_engine = RayWaContextFactory::create_engine(params.engine_type.clone());
        let ray_runtime = RayWaContextFactory::create_runtime(internal_cfg.clone());
        let (wasm_engine, ray_runtime) = tokio::join!(wasm_engine, ray_runtime);

        let mut context = RayWaContext {
            runtime: ray_runtime.unwrap(),
            engine: wasm_engine.unwrap(),
            params: params.clone(),
            config: internal_cfg,
        };

        context.runtime.do_init()?;
        context.engine.init()?;

        context.runtime.setup_hostcalls(&mut context.engine)?;

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

async fn run_binary(args: &util::LauncherParameters) -> Result<()>  {
    let ctx = RayWaContextFactory::create_context(&args);

    // check if wasm file exists
    let wasm_file = std::path::Path::new(&args.file);
    if !wasm_file.exists() {
        return Err(anyhow::anyhow!(
            "wasm file \"{}\" not found",
            wasm_file.display()
        ));
    }
    // if file found, read it and compile it
    let data = std::fs::read(wasm_file)?;

    // wait for context creation to finish
    let mut ctx = ctx.await?;

    let module = ctx.engine.compile("module", &data)?;
    let sandbox = ctx.engine.create_sandbox("sandbox")?;
    let instance = ctx.engine.instantiate("sandbox", "module", "instance")?;

    info!("wasm module instantiated");

    ctx.engine.execute("sandbox", "instance", "_start", vec![]);

    // for now, we just shutdown the runtime
    ctx.runtime.do_shutdown();
    Ok(())
}

async fn run_text(args: &util::LauncherParameters) -> Result<()> {
    unimplemented!()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let args = util::LauncherParameters::parse();
    
    // load data
    match args.file_format {
        util::WasmFileFormat::WASM => run_binary(&args).await?,
        util::WasmFileFormat::WAT => run_text(&args).await?,
    }

    Ok(())
}

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
mod cmd;
mod config;
mod ray;
mod util;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber;

async fn init(cfg: &ray::RayConfig, args: &cmd::Arguments) -> Result<()> {
    config::ConfigInternal::instance()
        .write()
        .unwrap()
        .init(&cfg, &args);
    {
        ray::RayRuntime::instance()
            .write()
            .unwrap()
            .do_init()
            .await
            .unwrap();
    }
    Ok(())
}

async fn run_task_loop() -> Result<()> {
    ray::RayRuntime::instance()
            .write()
            .unwrap()
            .launch_task_loop().await.unwrap();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let args = cmd::Arguments::parse();
    let cfg = ray::RayConfig::new();

    init(&cfg, &args).await.unwrap();
    run_task_loop().await.unwrap();
    Ok(())
}

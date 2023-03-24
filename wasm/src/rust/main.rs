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

use clap::Parser;
use tracing_subscriber;

fn init(cfg: &ray::RayConfig, args: &cmd::Arguments) {
    config::ConfigInternal::instance()
        .lock()
        .unwrap()
        .init(&cfg, &args);
    let rt = ray::RayRuntime::instance().lock().unwrap().do_init();
}

fn main() {
    tracing_subscriber::fmt().init();

    let args = cmd::Arguments::parse();
    let cfg = ray::RayConfig::new();

    init(&cfg, &args);
}

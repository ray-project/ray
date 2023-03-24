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
use prost_build;

struct Config {
    repo_root_dir: Option<std::path::PathBuf>,

    proto_src_files: Option<Vec<std::path::PathBuf>>,
    proto_inc_dirs: Option<Vec<std::path::PathBuf>>,
}

impl Config {
    fn new() -> Self {
        return Self {
            repo_root_dir: None,
            proto_src_files: None,
            proto_inc_dirs: None,
        };
    }

    fn init(&mut self) {
        self.fetch_project_root();
        self.fetch_proto_files();
        self.fetch_proto_file_dirs();
    }

    fn fetch_project_root(&mut self) {
        // get repo root path
        let repo_root_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .parent()
            .unwrap()
            .to_path_buf();
        self.repo_root_dir = Some(repo_root_dir.clone());
    }

    fn fetch_proto_files(&mut self) {
        match self.repo_root_dir {
            Some(_) => {}
            None => {
                panic!("no project root found!");
            }
        }
        // get source files by spliting the PROTO_SRCS environment variable
        let proto_src_files_relative = std::env::var("PROTO_SRCS")
            .unwrap()
            .split(' ')
            .map(|path| std::path::PathBuf::from(path))
            .collect::<Vec<_>>();
        // get proto source files absolute path
        let proto_src_files = proto_src_files_relative
            .iter()
            .map(|_path| self.repo_root_dir.as_ref().unwrap().clone().join(_path))
            .collect::<Vec<_>>();
        self.proto_src_files = Some(proto_src_files.clone());
    }

    fn fetch_proto_file_dirs(&mut self) {
        match self.proto_src_files {
            Some(_) => {}
            None => {
                panic!("no src files found!");
            }
        }
        // get the source directories
        let proto_src_dirs_dup = self
            .proto_src_files
            .as_ref()
            .unwrap()
            .iter()
            .map(|path| path.parent().unwrap())
            .collect::<Vec<_>>();
        // deduplicate the source directories
        let mut proto_inc_dirs = std::collections::HashSet::new();
        for dir in proto_src_dirs_dup {
            proto_inc_dirs.insert(dir);
        }
        // append repo root to source_dirs
        let binding = self.repo_root_dir.as_ref().unwrap().clone();
        proto_inc_dirs.insert(binding.as_path());

        self.proto_inc_dirs = Some(
            proto_inc_dirs
                .iter()
                .map(|path| path.clone().to_path_buf())
                .collect(),
        );
    }
}

fn main() {
    let mut config = Config::new();

    // initialize data from env vars
    config.init();

    // compile the protos, output to OUT_DIR
    prost_build::compile_protos(
        &config.proto_src_files.unwrap(),
        &config.proto_inc_dirs.unwrap(),
    )
    .unwrap();
}

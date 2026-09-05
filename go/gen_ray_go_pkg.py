# Copyright 2026 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from bazel.gen_extract import gen_extract

if __name__ == "__main__":
    gen_extract(
        [
            "go/ray_go_pkg.zip",
        ],
        clear_dir_first=[
            "ray/go",
        ],
        # Extract into the repository root's ray/go/ directory, matching the
        # ray-cpp design (no sub_dir parameter).
    )

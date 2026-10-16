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
import os

from bazel.gen_extract import gen_extract
from go.check_pkghash import verify_binaries

if __name__ == "__main__":
    gen_extract(
        [
            "go/ray_go_pkg.zip",
        ],
        # Clear only the build outputs, not the git-tracked cross-language
        # converter sources under ray/go/dashboard. Wipe the whole ray/go dir
        # would delete python/ray/go/dashboard/cross_language/*.py that are
        # tracked and shipped in the ray-go wheel.
        clear_dir_first=[
            "ray/go/cmd",
            "ray/go/lib",
        ],
        # Omit the sub_dir argument (gen_extract defaults to sub_dir="python"),
        # aligned with ray-cpp: the zip entries are ray/go/..., unpacked into
        # python/ray/go/{cmd,lib}, and shipped as part of the Ray package for
        # wheel building (-> site-packages/ray/go/).
    )
    # D4: fail the package step when raygo and go_runtime.so disagree on the
    # pkg/runtime/api package hash (the worker would otherwise exit silently
    # with a plugin version mismatch).
    # Note: gen_extract defaults to sub_dir="python", so the zip is unpacked
    # under <workspace_root>/python/ray/go/{cmd,lib}. `bazel run` does not
    # guarantee cwd == workspace root, so resolve paths explicitly against
    # BUILD_WORKSPACE_DIRECTORY (already validated by gen_extract above).
    root = os.environ["BUILD_WORKSPACE_DIRECTORY"]
    verify_binaries(
        [
            os.path.join(root, "python/ray/go/cmd/raygo"),
            os.path.join(root, "python/ray/go/lib/go_runtime.so"),
        ]
    )
    print("OK: all binaries share the same package hash")

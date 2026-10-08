// Copyright 2026 The Ray Authors.
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

package autoscaler

import (
	"fmt"
	"log"
	"os"
)

// CheckDockerFileMounts checks the file mount configuration and warns users
// against mounting single files directly.
func CheckDockerFileMounts(fileMounts map[string]interface{}) {
	for remote, local := range fileMounts {
		// Get the file info of the local path.
		fileInfo, err := os.Stat(local.(string))
		if err != nil {
			// If the path does not exist or cannot be accessed, skip the entry.
			continue
		}

		// Warn if the path is a file rather than a directory.
		if !fileInfo.IsDir() {
			log.Printf("[WARNING] File Mount: (%s:%s) refers to a file.\n"+
				"To ensure this mount updates properly, please use a directory.", remote, local)
		}
	}
}

// ValidateDockerConfig validates the Docker configuration.
func ValidateDockerConfig(config map[string]interface{}) error {
	// Return directly when there is no docker config.
	docker, ok := config["docker"].(map[string]interface{})
	if !ok {
		return nil
	}

	CheckDockerFileMounts(GetValueWithDefault[map[string]interface{}](config, make(map[string]interface{}), "file_mounts"))

	// Read the docker related fields.
	dockerImage, _ := docker["image"].(string)
	cName, _ := docker["container_name"].(string)
	headDockerImage, _ := docker["head_image"].(string)
	workerDockerImage, _ := docker["worker_image"].(string)

	imagePresent := (dockerImage != "") || ((headDockerImage != "") && (workerDockerImage != ""))
	if cName == "" && !imagePresent {
		return nil
	} else {
		if !(cName != "" && imagePresent) {
			return fmt.Errorf("must provide a container & image name")
		}
	}

	return nil
}

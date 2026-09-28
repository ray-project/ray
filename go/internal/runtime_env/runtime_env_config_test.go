// Copyright 2025 The Ray Authors.
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

package runtime_env

import (
	"testing"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	assert.NotNil(t, config)
	assert.Equal(t, common.DefaultRuntimeEnvTimeoutSeconds, config["setup_timeout_seconds"])
	assert.Equal(t, true, config["eager_install"])
	assert.Equal(t, []string{}, config["log_files"])
}

func TestNewRuntimeEnvConfig(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		config, err := NewRuntimeEnvConfig(300, false, []string{"file1.log", "file2.log"})
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, 300, config["setup_timeout_seconds"])
		assert.Equal(t, false, config["eager_install"])
		assert.Equal(t, []string{"file1.log", "file2.log"}, config["log_files"])
	})

	t.Run("timeout -1 means disable timeout", func(t *testing.T) {
		config, err := NewRuntimeEnvConfig(-1, true, []string{})
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, -1, config["setup_timeout_seconds"])
	})

	t.Run("nil logFiles converted to empty slice", func(t *testing.T) {
		config, err := NewRuntimeEnvConfig(600, true, nil)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, []string{}, config["log_files"])
	})

	t.Run("invalid timeout zero", func(t *testing.T) {
		config, err := NewRuntimeEnvConfig(0, true, []string{})
		assert.Error(t, err)
		assert.Nil(t, config)
		assert.Contains(t, err.Error(), "setup_timeout_seconds must be greater than zero or equals to -1")
	})

	t.Run("invalid timeout negative", func(t *testing.T) {
		config, err := NewRuntimeEnvConfig(-5, true, []string{})
		assert.Error(t, err)
		assert.Nil(t, config)
		assert.Contains(t, err.Error(), "setup_timeout_seconds must be greater than zero or equals to -1")
	})
}

func TestParseAndValidateRuntimeEnvConfig(t *testing.T) {
	t.Run("valid config with all fields", func(t *testing.T) {
		input := map[string]interface{}{
			"setup_timeout_seconds": 500,
			"eager_install":         false,
			"log_files":             []interface{}{"a.log", "b.log"},
		}
		config, err := ParseAndValidateRuntimeEnvConfig(input)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, 500, config["setup_timeout_seconds"])
		assert.Equal(t, false, config["eager_install"])
		assert.Equal(t, []string{"a.log", "b.log"}, config["log_files"])
	})

	t.Run("config with float64 timeout", func(t *testing.T) {
		input := map[string]interface{}{
			"setup_timeout_seconds": float64(400),
			"eager_install":         true,
			"log_files":             []interface{}{},
		}
		config, err := ParseAndValidateRuntimeEnvConfig(input)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, 400, config["setup_timeout_seconds"])
	})

	t.Run("empty config uses defaults", func(t *testing.T) {
		input := map[string]interface{}{}
		config, err := ParseAndValidateRuntimeEnvConfig(input)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, common.DefaultRuntimeEnvTimeoutSeconds, config["setup_timeout_seconds"])
		assert.Equal(t, true, config["eager_install"])
		assert.Equal(t, []string{}, config["log_files"])
	})

	t.Run("unknown fields are ignored", func(t *testing.T) {
		input := map[string]interface{}{
			"setup_timeout_seconds": 300,
			"unknown_field":         "value",
			"another_unknown":       123,
		}
		config, err := ParseAndValidateRuntimeEnvConfig(input)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, 300, config["setup_timeout_seconds"])
		_, exists := config["unknown_field"]
		assert.False(t, exists)
	})

	t.Run("invalid timeout in map returns error", func(t *testing.T) {
		input := map[string]interface{}{
			"setup_timeout_seconds": 0,
			"eager_install":         true,
			"log_files":             []interface{}{},
		}
		config, err := ParseAndValidateRuntimeEnvConfig(input)
		assert.Error(t, err)
		assert.Nil(t, config)
	})
}

func TestBuildProtoRuntimeEnvConfig(t *testing.T) {
	t.Run("build proto config with all fields", func(t *testing.T) {
		config := RuntimeEnvConfig{
			"setup_timeout_seconds": 300,
			"eager_install":         true,
			"log_files":             []string{"file1.log", "file2.log"},
		}
		protoConfig, err := BuildProtoRuntimeEnvConfig(config)
		assert.NoError(t, err)
		assert.NotNil(t, protoConfig)
		assert.Equal(t, int32(300), protoConfig.GetSetupTimeoutSeconds())
		assert.Equal(t, true, protoConfig.GetEagerInstall())
		assert.Equal(t, []string{"file1.log", "file2.log"}, protoConfig.GetLogFiles())
	})

	t.Run("build proto config with float64 timeout", func(t *testing.T) {
		config := RuntimeEnvConfig{
			"setup_timeout_seconds": float64(400),
			"eager_install":         false,
			"log_files":             []string{"a.log"},
		}
		protoConfig, err := BuildProtoRuntimeEnvConfig(config)
		assert.NoError(t, err)
		assert.NotNil(t, protoConfig)
		assert.Equal(t, int32(400), protoConfig.GetSetupTimeoutSeconds())
		assert.Equal(t, false, protoConfig.GetEagerInstall())
		assert.Equal(t, []string{"a.log"}, protoConfig.GetLogFiles())
	})

	t.Run("build proto config with empty config", func(t *testing.T) {
		config := RuntimeEnvConfig{}
		protoConfig, err := BuildProtoRuntimeEnvConfig(config)
		assert.NoError(t, err)
		assert.NotNil(t, protoConfig)
		assert.Equal(t, int32(0), protoConfig.GetSetupTimeoutSeconds())
		assert.Equal(t, false, protoConfig.GetEagerInstall())
		assert.Nil(t, protoConfig.GetLogFiles())
	})

	t.Run("build proto config with invalid setup_timeout_seconds type", func(t *testing.T) {
		config := RuntimeEnvConfig{
			"setup_timeout_seconds": "invalid",
		}
		protoConfig, err := BuildProtoRuntimeEnvConfig(config)
		assert.Error(t, err)
		assert.Nil(t, protoConfig)
		assert.Contains(t, err.Error(), "setup_timeout_seconds must be of type int or float64")
	})

	t.Run("build proto config with invalid eager_install type", func(t *testing.T) {
		config := RuntimeEnvConfig{
			"eager_install": "invalid",
		}
		protoConfig, err := BuildProtoRuntimeEnvConfig(config)
		assert.Error(t, err)
		assert.Nil(t, protoConfig)
		assert.Contains(t, err.Error(), "eager_install must be of type bool")
	})

	t.Run("build proto config with invalid log_files type", func(t *testing.T) {
		config := RuntimeEnvConfig{
			"log_files": "invalid",
		}
		protoConfig, err := BuildProtoRuntimeEnvConfig(config)
		assert.Error(t, err)
		assert.Nil(t, protoConfig)
		assert.Contains(t, err.Error(), "log_files must be of type []string or []interface{}")
	})

}

func TestFromProtoRuntimeEnvConfig(t *testing.T) {
	t.Run("from proto with all fields", func(t *testing.T) {
		protoConfig := &proto.RuntimeEnvConfig{
			SetupTimeoutSeconds: 500,
			EagerInstall:        false,
			LogFiles:            []string{"x.log", "y.log"},
		}
		config, err := FromProtoRuntimeEnvConfig(protoConfig)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, 500, config["setup_timeout_seconds"])
		assert.Equal(t, false, config["eager_install"])
		assert.Equal(t, []string{"x.log", "y.log"}, config["log_files"])
	})

	t.Run("from proto with zero timeout uses default", func(t *testing.T) {
		protoConfig := &proto.RuntimeEnvConfig{
			SetupTimeoutSeconds: 0,
			EagerInstall:        true,
			LogFiles:            []string{},
		}
		config, err := FromProtoRuntimeEnvConfig(protoConfig)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, common.DefaultRuntimeEnvTimeoutSeconds, config["setup_timeout_seconds"])
	})

	t.Run("from proto with nil log files", func(t *testing.T) {
		protoConfig := &proto.RuntimeEnvConfig{
			SetupTimeoutSeconds: 300,
			EagerInstall:        true,
			LogFiles:            nil,
		}
		config, err := FromProtoRuntimeEnvConfig(protoConfig)
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, []string{}, config["log_files"])
	})

	t.Run("ensures log_files is always []string type", func(t *testing.T) {
		protoConfig := &proto.RuntimeEnvConfig{
			SetupTimeoutSeconds: 300,
			EagerInstall:        true,
			LogFiles:            []string{"test.log"},
		}
		config, err := FromProtoRuntimeEnvConfig(protoConfig)
		assert.NoError(t, err)

		logFiles := config["log_files"]
		assert.NotNil(t, logFiles)

		_, ok := logFiles.([]string)
		assert.True(t, ok, "log_files should be []string type, not %T", logFiles)
	})
}

func TestToDict_RuntimeEnvConfig(t *testing.T) {
	t.Run("convert config to dict", func(t *testing.T) {
		config := RuntimeEnvConfig{
			"setup_timeout_seconds": 300,
			"eager_install":         true,
			"log_files":             []string{"a.log", "b.log"},
		}
		dict := ToDict(config)
		assert.NotNil(t, dict)
		assert.Equal(t, 300, dict["setup_timeout_seconds"])
		assert.Equal(t, true, dict["eager_install"])
		assert.Equal(t, []string{"a.log", "b.log"}, dict["log_files"])
	})

	t.Run("convert nil config returns nil", func(t *testing.T) {
		var config RuntimeEnvConfig = nil
		dict := ToDict(config)
		assert.Nil(t, dict)
	})

	t.Run("deep copy ensures independence", func(t *testing.T) {
		config := RuntimeEnvConfig{
			"log_files": []string{"a.log"},
			"nested":    map[string]interface{}{"key": "value"},
		}
		dict := ToDict(config)
		config["log_files"] = []string{"modified.log"}
		assert.Equal(t, []string{"a.log"}, dict["log_files"])
	})
}

func TestParseAndValidateRuntimeEnvConfigOption(t *testing.T) {
	t.Run("nil value returns nil", func(t *testing.T) {
		result, err := parseAndValidateRuntimeEnvConfigOption(nil)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("RuntimeEnvConfig type returns as is", func(t *testing.T) {
		input := RuntimeEnvConfig{"setup_timeout_seconds": 300}
		result, err := parseAndValidateRuntimeEnvConfigOption(input)
		assert.NoError(t, err)
		assert.Equal(t, input, result)
	})

	t.Run("map type converts to RuntimeEnvConfig", func(t *testing.T) {
		input := map[string]interface{}{
			"setup_timeout_seconds": 400,
			"eager_install":         false,
			"log_files":             []interface{}{"test.log"},
		}
		result, err := parseAndValidateRuntimeEnvConfigOption(input)
		assert.NoError(t, err)
		config, ok := result.(RuntimeEnvConfig)
		assert.True(t, ok)
		assert.Equal(t, 400, config["setup_timeout_seconds"])
	})

	t.Run("invalid type returns error", func(t *testing.T) {
		result, err := parseAndValidateRuntimeEnvConfigOption("invalid_string")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "runtime_env['config'] must be of type map or RuntimeEnvConfig")
	})

	t.Run("invalid map returns error", func(t *testing.T) {
		input := map[string]interface{}{"setup_timeout_seconds": 0}
		result, err := parseAndValidateRuntimeEnvConfigOption(input)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

package common_test

import (
	"os"
	"testing"

	"github.com/ray-project/ray/go/internal/common"
)

func TestEnvInteger(t *testing.T) {
	defaultVal := common.EnvInteger("NON_EXISTENT_KEY_12345", 42)
	if defaultVal != 42 {
		t.Errorf("Expected default value 42, got %d", defaultVal)
	}

	os.Setenv("TEST_INT_KEY", "100")
	defer os.Unsetenv("TEST_INT_KEY")
	val := common.EnvInteger("TEST_INT_KEY", 42)
	if val != 100 {
		t.Errorf("Expected 100, got %d", val)
	}

	os.Setenv("TEST_INT_INVALID", "not_a_number")
	defer os.Unsetenv("TEST_INT_INVALID")
	invalidVal := common.EnvInteger("TEST_INT_INVALID", 42)
	if invalidVal != 42 {
		t.Errorf("Expected default 42 for invalid int, got %d", invalidVal)
	}
}

func TestEnvFloat(t *testing.T) {
	defaultVal := common.EnvFloat("NON_EXISTENT_KEY_67890", 3.14)
	if defaultVal != 3.14 {
		t.Errorf("Expected default value 3.14, got %f", defaultVal)
	}

	os.Setenv("TEST_FLOAT_KEY", "2.71")
	defer os.Unsetenv("TEST_FLOAT_KEY")
	val := common.EnvFloat("TEST_FLOAT_KEY", 3.14)
	if val != 2.71 {
		t.Errorf("Expected 2.71, got %f", val)
	}

	os.Setenv("TEST_FLOAT_INVALID", "not_a_float")
	defer os.Unsetenv("TEST_FLOAT_INVALID")
	invalidVal := common.EnvFloat("TEST_FLOAT_INVALID", 3.14)
	if invalidVal != 3.14 {
		t.Errorf("Expected default 3.14 for invalid float, got %f", invalidVal)
	}
}

func TestEnvBool(t *testing.T) {
	defaultTrue := common.EnvBool("NON_EXISTENT_KEY_TRUE", true)
	if !defaultTrue {
		t.Error("Expected default value true")
	}

	defaultFalse := common.EnvBool("NON_EXISTENT_KEY_FALSE", false)
	if defaultFalse {
		t.Error("Expected default value false")
	}

	os.Setenv("TEST_BOOL_TRUE", "true")
	defer os.Unsetenv("TEST_BOOL_TRUE")
	valTrue := common.EnvBool("TEST_BOOL_TRUE", false)
	if !valTrue {
		t.Error("Expected true for 'true' value")
	}

	os.Setenv("TEST_BOOL_ONE", "1")
	defer os.Unsetenv("TEST_BOOL_ONE")
	valOne := common.EnvBool("TEST_BOOL_ONE", false)
	if !valOne {
		t.Error("Expected true for '1' value")
	}

	os.Setenv("TEST_BOOL_FALSE", "false")
	defer os.Unsetenv("TEST_BOOL_FALSE")
	valFalse := common.EnvBool("TEST_BOOL_FALSE", true)
	if valFalse {
		t.Error("Expected false for 'false' value")
	}

	os.Setenv("TEST_BOOL_OTHER", "other")
	defer os.Unsetenv("TEST_BOOL_OTHER")
	valOther := common.EnvBool("TEST_BOOL_OTHER", true)
	if valOther {
		t.Error("Expected false for 'other' value")
	}
}

func TestEnvSetByUser(t *testing.T) {
	if common.EnvSetByUser("NON_EXISTENT_KEY_ABC") {
		t.Error("Expected false for non-existent key")
	}

	os.Setenv("TEST_SET_KEY", "some_value")
	defer os.Unsetenv("TEST_SET_KEY")
	if !common.EnvSetByUser("TEST_SET_KEY") {
		t.Error("Expected true for existing key")
	}

	os.Setenv("TEST_EMPTY_KEY", "")
	defer os.Unsetenv("TEST_EMPTY_KEY")
	if !common.EnvSetByUser("TEST_EMPTY_KEY") {
		t.Error("Expected true for empty value key")
	}
}

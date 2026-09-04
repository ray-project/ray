package common

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRayHomeAndPath(t *testing.T) {
	if RAY_PATH == "" {
		t.Error("RAY_PATH should not be empty")
	}

	info, err := os.Stat(RAY_PATH)
	if err != nil {
		t.Errorf("RAY_PATH should be a valid directory: %v", err)
	} else if !info.IsDir() {
		t.Error("RAY_PATH should be a directory")
	}
}

func TestGetRayJarsDir(t *testing.T) {
	originalRayPath := RAY_PATH
	defer func() {
		RAY_PATH = originalRayPath
	}()

	t.Run("jars directory not exists", func(t *testing.T) {
		tempDir := t.TempDir()
		RAY_PATH = tempDir

		_, err := GetRayJarsDir()
		if err == nil {
			t.Error("GetRayJarsDir should return error when jars directory does not exist")
		}
	})

	t.Run("jars directory exists", func(t *testing.T) {
		tempDir := t.TempDir()
		jarsDir := filepath.Join(tempDir, "jars")
		err := os.Mkdir(jarsDir, 0755)
		if err != nil {
			t.Fatalf("failed to create jars dir: %v", err)
		}

		RAY_PATH = tempDir

		result, err := GetRayJarsDir()
		if err != nil {
			t.Errorf("GetRayJarsDir should not return error: %v", err)
		}

		expectedPath, _ := filepath.Abs(jarsDir)
		if result != expectedPath {
			t.Errorf("GetRayJarsDir = %q, want %q", result, expectedPath)
		}
	})
}

func TestGetRayJarsDirWithEnv(t *testing.T) {
	originalRayPath := os.Getenv("RAY_PATH")
	originalRayPathVar := RAY_PATH
	defer func() {
		if originalRayPath == "" {
			os.Unsetenv("RAY_PATH")
		} else {
			os.Setenv("RAY_PATH", originalRayPath)
		}
		RAY_PATH = originalRayPathVar
	}()

	tempDir := t.TempDir()
	jarsDir := filepath.Join(tempDir, "jars")
	err := os.Mkdir(jarsDir, 0755)
	if err != nil {
		t.Fatalf("failed to create jars dir: %v", err)
	}

	os.Setenv("RAY_PATH", tempDir)

	// Re-initialize RAY_PATH to mirror what the package init function does.
	RAY_PATH = os.Getenv("RAY_PATH")

	result, err := GetRayJarsDir()
	if err != nil {
		t.Errorf("GetRayJarsDir should not return error: %v", err)
	}

	expectedPath, _ := filepath.Abs(jarsDir)
	if result != expectedPath {
		t.Errorf("GetRayJarsDir = %q, want %q", result, expectedPath)
	}
}

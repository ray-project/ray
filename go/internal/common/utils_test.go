package common

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInterfaceToStringSlice(t *testing.T) {
	t.Run("normal conversion", func(t *testing.T) {
		input := []interface{}{"a", "b", "c"}
		result := InterfaceToStringSlice(input)
		assert.Equal(t, []string{"a", "b", "c"}, result)
	})

	t.Run("nil input returns nil", func(t *testing.T) {
		var input []interface{} = nil
		result := InterfaceToStringSlice(input)
		assert.Nil(t, result)
	})

	t.Run("empty slice", func(t *testing.T) {
		input := []interface{}{}
		result := InterfaceToStringSlice(input)
		assert.Equal(t, []string{}, result)
	})

	t.Run("mixed types skips non-strings", func(t *testing.T) {
		input := []interface{}{"a", 123, "b", true, "c"}
		result := InterfaceToStringSlice(input)
		assert.Equal(t, []string{"a", "", "b", "", "c"}, result)
	})
}

func TestInterfaceMapToStringMap(t *testing.T) {
	t.Run("normal conversion", func(t *testing.T) {
		input := map[string]interface{}{"key1": "val1", "key2": "val2"}
		result := InterfaceMapToStringMap(input)
		assert.Equal(t, map[string]string{"key1": "val1", "key2": "val2"}, result)
	})

	t.Run("nil input returns nil", func(t *testing.T) {
		var input map[string]interface{} = nil
		result := InterfaceMapToStringMap(input)
		assert.Nil(t, result)
	})

	t.Run("empty map", func(t *testing.T) {
		input := map[string]interface{}{}
		result := InterfaceMapToStringMap(input)
		assert.Equal(t, map[string]string{}, result)
	})

	t.Run("mixed types skips non-strings", func(t *testing.T) {
		input := map[string]interface{}{
			"key1": "val1",
			"key2": 123,
			"key3": true,
			"key4": "val4",
		}
		result := InterfaceMapToStringMap(input)
		assert.Equal(t, map[string]string{"key1": "val1", "key4": "val4"}, result)
	})
}

func TestDirSizeBytes(t *testing.T) {
	t.Run("empty directory returns zero", func(t *testing.T) {
		tmpDir := t.TempDir()
		size, err := DirSizeBytes(tmpDir)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), size)
	})

	t.Run("single file", func(t *testing.T) {
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		content := []byte("hello world")
		err := os.WriteFile(testFile, content, 0644)
		assert.NoError(t, err)

		size, err := DirSizeBytes(tmpDir)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(content)), size)
	})

	t.Run("multiple files", func(t *testing.T) {
		tmpDir := t.TempDir()
		file1 := filepath.Join(tmpDir, "file1.txt")
		file2 := filepath.Join(tmpDir, "file2.txt")
		content1 := []byte("content1")
		content2 := []byte("content2")

		err := os.WriteFile(file1, content1, 0644)
		assert.NoError(t, err)
		err = os.WriteFile(file2, content2, 0644)
		assert.NoError(t, err)

		size, err := DirSizeBytes(tmpDir)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(content1)+len(content2)), size)
	})

	t.Run("nested directory structure", func(t *testing.T) {
		tmpDir := t.TempDir()
		subDir := filepath.Join(tmpDir, "subdir")
		nestedDir := filepath.Join(subDir, "nested")
		err := os.MkdirAll(nestedDir, 0755)
		assert.NoError(t, err)

		file1 := filepath.Join(tmpDir, "root.txt")
		file2 := filepath.Join(subDir, "sub.txt")
		file3 := filepath.Join(nestedDir, "deep.txt")
		content1 := []byte("root")
		content2 := []byte("sub")
		content3 := []byte("deep")

		err = os.WriteFile(file1, content1, 0644)
		assert.NoError(t, err)
		err = os.WriteFile(file2, content2, 0644)
		assert.NoError(t, err)
		err = os.WriteFile(file3, content3, 0644)
		assert.NoError(t, err)

		size, err := DirSizeBytes(tmpDir)
		assert.NoError(t, err)
		expectedSize := int64(len(content1) + len(content2) + len(content3))
		assert.Equal(t, expectedSize, size)
	})

	t.Run("non-existent directory returns error", func(t *testing.T) {
		_, err := DirSizeBytes("/nonexistent/directory/path")
		assert.Error(t, err)
	})

	t.Run("file instead of directory returns file size", func(t *testing.T) {
		// DirSizeBytes uses filepath.WalkDir, so passing a file returns that file's size.
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		content := []byte("test content")
		err := os.WriteFile(testFile, content, 0644)
		assert.NoError(t, err)

		size, err := DirSizeBytes(testFile)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(content)), size)
	})

	t.Run("large files", func(t *testing.T) {
		tmpDir := t.TempDir()
		largeFile := filepath.Join(tmpDir, "large.bin")
		largeContent := make([]byte, 1024*1024)
		err := os.WriteFile(largeFile, largeContent, 0644)
		assert.NoError(t, err)

		size, err := DirSizeBytes(tmpDir)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(largeContent)), size)
	})

	t.Run("hidden files included", func(t *testing.T) {
		tmpDir := t.TempDir()
		hiddenFile := filepath.Join(tmpDir, ".hidden")
		content := []byte("hidden content")
		err := os.WriteFile(hiddenFile, content, 0644)
		assert.NoError(t, err)

		size, err := DirSizeBytes(tmpDir)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(content)), size)
	})
}

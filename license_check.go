package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/licensecheck"
)

func GetLicense(filepath string) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Println("Error reading license file:", err)
		return
	}

	// Scan the file with licensecheck
	result := licensecheck.Scan(data)

	// Print basic summary
	fmt.Printf("License coverage: %.1f%%\n", result.Percent)
	if len(result.Match) == 0 {
		fmt.Println("No license match found.")
		return
	}

	// Print info about each license match
	for _, m := range result.Match {
		fmt.Printf("For file %s, license ID: %s, type = %s\n", filepath, m.ID, m.Type.String())
	}
}

func main() {
	root := "/home/ubuntu/.cache/bazel/_bazel_ubuntu/2139a9ac91f0a6877cfd4920666e807a/external"

	dirEntries, err := os.ReadDir(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read root dir: %v\n", err)
		os.Exit(1)
	}

	// Create channel and waitgroup
	fileChan := make(chan string, 50)
	var wg sync.WaitGroup

	// Worker goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for path := range fileChan {
			GetLicense(path)
		}
	}()

	// Producer: iterate subdirectories
	for _, dirEntry := range dirEntries {
		if !dirEntry.IsDir() {
			continue
		}
		fileEntries, err := os.ReadDir(filepath.Join(root, dirEntry.Name()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read dir: %v\n", err)
			os.Exit(1)
		}
		for _, fileEntry := range fileEntries {
			if fileEntry.IsDir() {
				continue
			}
			if strings.Contains(fileEntry.Name(), "LICENSE") {
				licensePath := filepath.Join(root, dirEntry.Name(), fileEntry.Name())
				fileChan <- licensePath
			}
		}
	}

	close(fileChan)
	wg.Wait()
}

package main

import (
	"fmt"
	"os"
	"path/filepath"
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
	// Define the root directory
	root := "/home/ubuntu/.cache/bazel/_bazel_ubuntu/2139a9ac91f0a6877cfd4920666e807a/external"

	// Create a channel for communication between Goroutines
	filePathChannel := make(chan string, 100)

	// WaitGroup to wait for all Goroutines to complete
	var wg sync.WaitGroup

	// Goroutine-2: The listener Goroutine that processes the files
	wg.Add(1)
	go func() {
		defer wg.Done()
		for filePath := range filePathChannel {
			GetLicense(filePath)
		}
	}()

	// Goroutine-1: The directory walker that sends file paths to the channel
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "error accessing path %q: %v\n", path, err)
			return nil // Continue walking
		}
		if !d.IsDir() && d.Name() == "LICENSE" {
			// Send the file path to the channel
			filePathChannel <- path
		}
		return nil
	})

	// Close the channel when the walking is done
	if err != nil {
		fmt.Fprintf(os.Stderr, "error walking the path %q: %v\n", root, err)
		os.Exit(1)
	}

	// Close the channel once all file paths have been sent
	close(filePathChannel)

	// Wait for the listener Goroutine to finish processing
	wg.Wait()
}

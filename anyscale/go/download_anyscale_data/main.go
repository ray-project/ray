package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
)

func main() {
	uds := flag.String(
		"sock", "/tmp/anyscale/anyscaled/sockets/dataplane_service.sock",
		"Dataplane service unix domain socket",
	)
	out := flag.String("out", "-", "Output file, default to stdout")
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		log.Fatalf("Usage: %s <path>", args[0])
	}

	var w io.Writer
	if *out == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(*out)
		if err != nil {
			log.Fatalf("create output file: %v", err)
		}
		defer f.Close()
		defer func() {
			if err := f.Sync(); err != nil {
				log.Fatalf("sync output file: %v", err)
			}
		}()
		w = f
	}

	ctx := context.Background()
	if err := downloadAnyscaleData(ctx, *uds, args[0], w); err != nil {
		log.Fatal(err)
	}
}

func downloadAnyscaleData(ctx context.Context, uds, p string, out io.Writer) error {
	dialFunc := func(ctx context.Context, n, addr string) (net.Conn, error) {
		return net.Dial("unix", uds)
	}
	client := &http.Client{
		Transport: &http.Transport{DialContext: dialFunc},
	}

	reqBody, err := json.Marshal(map[string]string{"key": p})
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequest(
		http.MethodPost, "http://unix/presigned_urls",
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return fmt.Errorf("make request: %w", err)
	}
	req.Header = make(http.Header)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed: %s: %s", resp.Status, body)
	}

	var response struct {
		URL    string `json:"url"`
		Method string `json:"method"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("decode response body: %w", err)
	}

	if response.Method != http.MethodGet {
		return fmt.Errorf("unexpected method: %q", response.Method)
	}

	// Use the default client to perform the download
	downloadResp, err := http.Get(response.URL)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer downloadResp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("download failed: %s: %s", resp.Status, body)
	}

	if _, err := io.Copy(out, downloadResp.Body); err != nil {
		return fmt.Errorf("write to output: %w", err)
	}

	return nil
}

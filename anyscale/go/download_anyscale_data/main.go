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
	"net/url"
	"os"
)

func main() {
	uds := flag.String(
		"sock", "/tmp/anyscale/anyscaled/sockets/dataplane_service.sock",
		"Dataplane service unix domain socket",
	)
	usePresigned := flag.Bool(
		"use_presigned", false, "Use presigned URL method",
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

	p := args[0]
	ctx := context.Background()
	if !*usePresigned {
		if err := downloadAnyscaleData(ctx, *uds, p, w); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := downloadAnyscaleDataUsingPresigned(
			ctx, *uds, p, w,
		); err != nil {
			log.Fatal(err)
		}
	}
}

func presignObjectURL(
	ctx context.Context, client *http.Client, p string,
) (string, error) {
	reqBody, err := json.Marshal(map[string]string{"key": p})
	if err != nil {
		return "", fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequest(
		http.MethodPost, "http://unix/presigned_urls",
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return "", fmt.Errorf("make request: %w", err)
	}
	req.Header = make(http.Header)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("request failed: %s: %s", resp.Status, body)
	}

	var response struct {
		URL    string `json:"url"`
		Method string `json:"method"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}

	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("decode response body: %w", err)
	}

	if response.Method != http.MethodGet {
		return "", fmt.Errorf("unexpected method: %q", response.Method)
	}

	return response.URL, nil
}

func pipeDownload(
	ctx context.Context, client *http.Client, url *url.URL, w io.Writer,
) error {
	req := &http.Request{
		Method: http.MethodGet,
		URL:    url,
	}

	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed: %s: %s", resp.Status, body)
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		return fmt.Errorf("write to output: %w", err)
	}

	return nil
}

func newUnixHTTPClient(uds string) *http.Client {
	dialFunc := func(ctx context.Context, n, addr string) (net.Conn, error) {
		return net.Dial("unix", uds)
	}
	return &http.Client{
		Transport: &http.Transport{DialContext: dialFunc},
	}
}

func downloadAnyscaleData(
	ctx context.Context, uds, p string, out io.Writer,
) error {
	client := newUnixHTTPClient(uds)

	q := make(url.Values)
	q.Set("key", p)

	u := &url.URL{
		Scheme:   "http",
		Host:     "unix", // dummy host for unix domain socket,
		Path:     "/get_object",
		RawQuery: q.Encode(),
	}

	return pipeDownload(ctx, client, u, out)
}

func downloadAnyscaleDataUsingPresigned(
	ctx context.Context, uds, p string, out io.Writer,
) error {
	client := newUnixHTTPClient(uds)

	urlStr, err := presignObjectURL(ctx, client, p)
	if err != nil {
		return fmt.Errorf("presign object URL: %w", err)
	}

	urlParsed, err := url.Parse(urlStr)
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	// Use the default client to perform the download.
	return pipeDownload(ctx, http.DefaultClient, urlParsed, out)
}

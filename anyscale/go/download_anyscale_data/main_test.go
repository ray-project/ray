package main

import (
	"testing"

	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
)

type fakeService struct {
	addr     string
	dataPath string
}

func (s *fakeService) handleDataplane(
	w http.ResponseWriter, req *http.Request,
) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.URL.Path != "/presigned_urls" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	var reqData struct {
		Key string `json:"key"`
	}

	reqBody, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
		return
	}

	if err := json.Unmarshal(reqBody, &reqData); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("unmarshal: %s", err))
		return
	}

	if reqData.Key != s.dataPath {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, fmt.Sprintf("unexpected key: %q", reqData.Key))
		return
	}

	respBody, err := json.Marshal(map[string]string{
		"url":    fmt.Sprintf("http://%s/data", s.addr),
		"method": "GET",
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(respBody); err != nil {
		log.Print("respond error: ", err)
	}
}

func (s *fakeService) handleDownload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.URL.Path != "/data" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, "hello"); err != nil {
		log.Print("respond error: ", err)
	}
}

func TestDownloadAnyscaleData(t *testing.T) {
	tmp := t.TempDir()

	s := &fakeService{dataPath: "ray-opt.tgz"}

	dl := httptest.NewServer(http.HandlerFunc(s.handleDownload))
	defer dl.Close()

	s.addr = dl.Listener.Addr().String()

	sock := filepath.Join(tmp, "dataplane.sock")
	srv := httptest.NewUnstartedServer(http.HandlerFunc(s.handleDataplane))
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal("listen dataplane socket: ", err)
	}
	srv.Listener = lis
	srv.Start()
	defer srv.Close()

	buf := new(bytes.Buffer)
	ctx := context.Background()
	if err := downloadAnyscaleData(ctx, sock, "ray-opt.tgz", buf); err != nil {
		t.Fatal("download: ", err)
	}

	got := buf.String()
	if got != "hello" {
		t.Errorf("got %q, want `hello`", got)
	}

	if err := downloadAnyscaleData(
		ctx, sock, "foobar", io.Discard,
	); err == nil {
		t.Error("expected error, got nil")
	}
}

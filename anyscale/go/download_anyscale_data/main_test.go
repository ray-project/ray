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
	"net/url"
	"path/filepath"
)

type fakeService struct {
	addr     string
	dataPath string

	downloadDisabled bool
}

func (s *fakeService) handleDataplane(
	w http.ResponseWriter, req *http.Request,
) {

	switch req.URL.Path {
	default:
		w.WriteHeader(http.StatusNotFound)
		return

	case "/presigned_urls":
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
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

	case "/get_object":
		if req.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		k := req.URL.Query().Get("key")
		if k != s.dataPath {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "key %q not found", k)
			return
		}

		if _, err := io.WriteString(w, "hello"); err != nil {
			log.Print("respond error: ", err)
		}
	}
}

func (s *fakeService) handleDownload(w http.ResponseWriter, req *http.Request) {
	if s.downloadDisabled {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if req.URL.Path != "/data" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if _, err := io.WriteString(w, "hello"); err != nil {
		log.Print("respond error: ", err)
	}
}

func TestDownloadAnyscaleDataUsingPresigned(t *testing.T) {
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
	if err := downloadAnyscaleDataUsingPresigned(
		ctx, sock, "ray-opt.tgz", buf,
	); err != nil {
		t.Fatal("download: ", err)
	}

	got := buf.String()
	if got != "hello" {
		t.Errorf("got %q, want `hello`", got)
	}

	if err := downloadAnyscaleDataUsingPresigned(
		ctx, sock, "foobar", io.Discard,
	); err == nil {
		t.Error("got no error, want error")
	}

	s.downloadDisabled = true
	if err := downloadAnyscaleDataUsingPresigned(
		ctx, sock, "ray-opt.tgz", io.Discard,
	); err == nil {
		t.Error("got nil error when download is disabled, want error")
	}
}

func TestPipeDownload(t *testing.T) {
	handler := func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if req.URL.Path != "/data" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if _, err := io.WriteString(w, "hello"); err != nil {
			log.Print("respond error: ", err)
		}
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	client := http.DefaultClient
	ctx := context.Background()
	buf := new(bytes.Buffer)
	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatal("parse url: ", err)
	}

	u.Path = "/data"
	if err := pipeDownload(ctx, client, u, buf); err != nil {
		t.Error("got error: ", err)
	}

	u.Path = "/not-exist"
	if err := pipeDownload(ctx, client, u, io.Discard); err == nil {
		t.Error("got no error when path not-exist, want error")
	}
}

func TestDownloadAnyscaleDataDirect(t *testing.T) {
	tmp := t.TempDir()

	s := &fakeService{dataPath: "ray-opt.tgz"}

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
	if want := "hello"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	if err := downloadAnyscaleData(ctx, sock, "na", io.Discard); err == nil {
		t.Error("got no error when downloading non-exist, want error")
	}
}

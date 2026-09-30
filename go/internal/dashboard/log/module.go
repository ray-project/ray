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

package log

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// LogAgentModule implements both the HTTP static file serving (mirroring
// Python's LogAgent in log_agent.py:244-255) and the LogService gRPC API
// (mirroring Python's LogAgentV1Grpc in log_agent.py:263-404). It serves
// the file browser over /logs/* and provides the ListLogs and StreamLog
// gRPC methods.
type LogAgentModule struct {
	UnimplementedLogServiceServer
	logDir string
}

// Compile-time checks that LogAgentModule satisfies both contracts.
var (
	_ agent.Module     = (*LogAgentModule)(nil)
	_ LogServiceServer = (*LogAgentModule)(nil)
)

// New builds a LogAgentModule from the agent config, verifying the configured
// log directory exists up front so misconfiguration surfaces at startup.
func New(cfg agent.Config) (*LogAgentModule, error) {
	if cfg.LogDir == "" {
		return nil, fmt.Errorf("log dir is empty")
	}
	info, err := os.Stat(cfg.LogDir)
	if err != nil {
		return nil, fmt.Errorf("stat log dir %q: %w", cfg.LogDir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("log dir %q is not a directory", cfg.LogDir)
	}
	return &LogAgentModule{logDir: cfg.LogDir}, nil
}

// Start prepares the module for serving. The Python counterparts (LogAgent
// and LogAgentV1Grpc) have no initialization step, so this is a no-op.
func (a *LogAgentModule) Start(ctx context.Context) error {
	return nil
}

// RegisterGRPC registers the LogService servicer on the agent's gRPC server,
// mirroring Python's LogAgentV1Grpc.run in log_agent.py:267-269.
func (a *LogAgentModule) RegisterGRPC(s *grpc.Server) error {
	RegisterLogServiceServer(s, a)
	return nil
}

// RegisterHTTP registers the static /logs/ file browser served by the agent's
// HTTP server, mirroring Python's LogAgent.__init__ in log_agent.py:247-248.
func (a *LogAgentModule) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("/logs/", a.handleLogs)
	return nil
}

// ListLogs returns all entries in the active Ray logs directory matching the
// requested glob filter, as paths relative to the log directory. Directories
// are given a trailing slash so callers can distinguish them from files. It
// mirrors LogAgentV1Grpc.ListLogs in log_agent.py:280-297.
//
// NOTE: Go's filepath.Glob, like Python's pathlib glob without recursive
// mode, does not treat ** as a path-spanning wildcard; a pattern such as
// "**/*.err" only matches one level deep. Current callers use flat filters
// (e.g. "*", "*worker*"), so this is not a practical gap.
func (a *LogAgentModule) ListLogs(ctx context.Context, req *proto.ListLogsRequest) (*proto.ListLogsReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}
	info, err := os.Stat(a.logDir)
	if err != nil || !info.IsDir() {
		return nil, status.Error(codes.NotFound, fmt.Sprintf(
			"could not find log dir at path: %s. It is unexpected. Please report an issue to Ray Github.",
			a.logDir))
	}
	matches, err := filepath.Glob(filepath.Join(a.logDir, req.GlobFilter))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid glob filter: "+err.Error())
	}
	logFiles := make([]string, 0, len(matches))
	for _, m := range matches {
		rel, err := filepath.Rel(a.logDir, m)
		if err != nil {
			log.Log.Error(err, "failed to relativize matched log path", "path", m)
			continue
		}
		suffix := ""
		if fi, err := os.Stat(m); err == nil && fi.IsDir() {
			suffix = "/"
		}
		logFiles = append(logFiles, rel+suffix)
	}
	return &proto.ListLogsReply{LogFiles: logFiles}, nil
}

// handleLogs serves files and directory listings under /logs/ from the module
// log directory, refusing anything that escapes it. This mirrors Python's
// LogAgent.routes.static("/logs", ...) in log_agent.py:248.
func (a *LogAgentModule) handleLogs(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/logs/")
	resolved, err := resolveLogPath(a.logDir, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := os.Stat(resolved); err != nil {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, resolved)
}

// These stream parameters mirror the constants in log_agent.py. blockSize is
// declared in log_agent.go.
const (
	// defaultKeepAliveIntervalSec is the poll interval used when following the
	// tail of a log file without an explicit interval.
	defaultKeepAliveIntervalSec = 1
	// defaultTailLines is the number of lines tailed unless the caller
	// overrides it.
	defaultTailLines = 1000
)

// StreamLog streams the tail of a log file to the client, mirroring
// LogAgentV1Grpc.StreamLog in log_agent.py:335-404. Without keep_alive it terminates
// once endOffset (default end of file) is reached; with keep_alive it keeps
// waiting for appended content. When the requested filename cannot be resolved
// to a real file inside the log directory, the LOG_GRPC_ERROR metadata header
// is sent instead of stream data, which the client raises on.
func (a *LogAgentModule) StreamLog(req *proto.StreamLogRequest, stream LogService_StreamLogServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is nil")
	}
	lines := int64(defaultTailLines)
	if req.Lines != nil && *req.Lines != 0 {
		lines = int64(*req.Lines)
	}

	filename, err := resolveFilename(a.logDir, req.LogFileName)
	if err != nil {
		return streamLogError(stream, err)
	}
	f, err := os.Open(filename)
	if err != nil {
		return streamLogError(stream, err)
	}
	defer f.Close()

	startOffset := int64(0)
	if req.StartOffset != nil {
		startOffset = int64(*req.StartOffset)
	}
	endOffset, err := findEndOffsetFile(f)
	if err != nil {
		return err
	}
	if req.EndOffset != nil {
		endOffset = int64(*req.EndOffset)
	}

	if lines != -1 {
		last, err := findStartOffsetLastNLinesFromOffset(f, endOffset, lines, blockSize)
		if err != nil {
			return err
		}
		startOffset = max(last, startOffset)
	}

	keepAliveInterval := float32(-1)
	if req.KeepAlive {
		keepAliveInterval = defaultKeepAliveIntervalSec
		if req.Interval != nil && *req.Interval != 0 {
			keepAliveInterval = *req.Interval
		}
		// While following, the stream reads beyond the current end of file.
		endOffset = -1
	}

	log.Log.Info("tailing log file",
		"path", filename,
		"start_offset", startOffset,
		"end_offset", endOffset,
		"lines", lines,
		"keep_alive_interval_sec", keepAliveInterval)
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}
	return streamLogInChunk(stream.Context(), stream, f, startOffset, endOffset, keepAliveInterval, blockSize)
}

// streamLogError reports an unresolvable log file through the LOG_GRPC_ERROR
// metadata header, matching how log_agent.py surfaces resolution failures on a
// successful RPC rather than a gRPC error code.
func streamLogError(stream LogService_StreamLogServer, err error) error {
	if herr := stream.SendHeader(metadata.Pairs(logGRPCError, err.Error())); herr != nil {
		return herr
	}
	return nil
}

// streamLogInChunk reads and sends the file in blockSize chunks from
// startOffset until endOffset (the file end when -1) or, with a non-negative
// keepAliveInterval, keeps polling for appended content at EOF. It mirrors
// _stream_log_in_chunk in log_agent.py.
func streamLogInChunk(
	ctx context.Context,
	stream LogService_StreamLogServer,
	f *os.File,
	startOffset int64,
	endOffset int64,
	keepAliveIntervalSec float32,
	blockSize int64,
) error {
	if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
		return err
	}
	curOffset := startOffset
	buf := make([]byte, blockSize)
	for {
		if ctx.Err() != nil {
			return nil
		}
		toRead := blockSize
		if endOffset != -1 {
			toRead = max(0, min(endOffset-curOffset, blockSize))
			if toRead == 0 {
				return nil
			}
		}
		n, err := f.Read(buf[:toRead])
		if n > 0 {
			if serr := stream.Send(&proto.StreamLogReply{Data: buf[:n]}); serr != nil {
				return serr
			}
			curOffset += int64(n)
			if endOffset != -1 && curOffset >= endOffset {
				return nil
			}
		}
		if err != nil {
			if err == io.EOF {
				if keepAliveIntervalSec >= 0 {
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(time.Duration(keepAliveIntervalSec * float32(time.Second))):
					}
					continue
				}
				return nil
			}
			return err
		}
	}
}

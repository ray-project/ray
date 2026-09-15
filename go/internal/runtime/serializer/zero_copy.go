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

package serializer

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// ZeroCopyWriter provides zero-copy serialization for large objects.
// Similar to C++'s zero-copy optimization for performance-critical paths.
//
// This writer avoids intermediate buffer allocations by writing directly
// to a pre-allocated buffer, reducing memory pressure and GC overhead.
type ZeroCopyWriter struct {
	buf []byte
	pos int
}

// NewZeroCopyWriter creates a new zero-copy writer with initial capacity.
func NewZeroCopyWriter(initialCap int) *ZeroCopyWriter {
	return &ZeroCopyWriter{
		buf: make([]byte, 0, initialCap),
		pos: 0,
	}
}

// Write implements io.Writer interface.
// Directly copies data to internal buffer without intermediate allocation.
func (w *ZeroCopyWriter) Write(p []byte) (int, error) {
	// Ensure capacity
	if w.pos+len(p) > cap(w.buf) {
		// Grow buffer with doubling strategy
		newCap := cap(w.buf) * 2
		if newCap < w.pos+len(p) {
			newCap = w.pos + len(p)
		}
		if newCap < 1024 {
			newCap = 1024
		}
		newBuf := make([]byte, w.pos+len(p), newCap)
		if w.pos > 0 {
			copy(newBuf, w.buf[:w.pos])
		}
		w.buf = newBuf
	} else {
		// Extend buffer length to accommodate new data
		if w.pos+len(p) > len(w.buf) {
			w.buf = w.buf[:w.pos+len(p)]
		}
	}

	// Direct copy to buffer (zero intermediate allocation)
	n := copy(w.buf[w.pos:], p)
	w.pos += n

	return n, nil
}

// Buffer returns the underlying buffer.
// Caller should use buf[:w.pos] to get written data.
func (w *ZeroCopyWriter) Buffer() []byte {
	return w.buf[:w.pos]
}

// Reset resets the writer position to 0.
// Buffer capacity is retained for reuse.
func (w *ZeroCopyWriter) Reset() {
	w.pos = 0
}

// Capacity returns the current buffer capacity.
func (w *ZeroCopyWriter) Capacity() int {
	return cap(w.buf)
}

// Len returns the number of bytes written.
func (w *ZeroCopyWriter) Len() int {
	return w.pos
}

// ZeroCopyReader provides zero-copy deserialization from byte slice.
// Avoids creating intermediate byte slices during decoding.
type ZeroCopyReader struct {
	buf []byte
	pos int
}

// NewZeroCopyReader creates a new zero-copy reader from byte slice.
func NewZeroCopyReader(data []byte) *ZeroCopyReader {
	return &ZeroCopyReader{
		buf: data,
		pos: 0,
	}
}

// Read implements io.Reader interface.
// Directly reads from internal buffer without allocation.
func (r *ZeroCopyReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}

	n := copy(p, r.buf[r.pos:])
	r.pos += n

	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// ReadByte reads a single byte.
func (r *ZeroCopyReader) ReadByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// ReadFull reads exactly len(buf) bytes.
func (r *ZeroCopyReader) ReadFull(buf []byte) error {
	if r.pos+len(buf) > len(r.buf) {
		return io.ErrUnexpectedEOF
	}
	copy(buf, r.buf[r.pos:r.pos+len(buf)])
	r.pos += len(buf)
	return nil
}

// Reset resets the reader position to 0.
func (r *ZeroCopyReader) Reset(data []byte) {
	r.buf = data
	r.pos = 0
}

// Len returns the number of bytes remaining.
func (r *ZeroCopyReader) Len() int {
	return len(r.buf) - r.pos
}

// ZeroCopySerializer provides zero-copy serialization with 9-byte header.
type ZeroCopySerializer struct {
	writer *ZeroCopyWriter
	reader *ZeroCopyReader
}

// NewZeroCopySerializer creates a new zero-copy serializer.
func NewZeroCopySerializer() *ZeroCopySerializer {
	return &ZeroCopySerializer{
		writer: NewZeroCopyWriter(DefaultInitialBufferSize),
		reader: nil,
	}
}

// EncodeZeroCopy serializes object with zero-copy optimization.
// Returns a slice of the internal buffer (no additional allocation).
func (s *ZeroCopySerializer) EncodeZeroCopy(obj interface{}) ([]byte, error) {
	// Reset writer position (retain capacity)
	s.writer.Reset()

	// Ensure buffer has at least MessagePackOffset capacity
	if cap(s.writer.buf) < MessagePackOffset {
		s.writer.buf = make([]byte, 0, DefaultInitialBufferSize)
	}

	// Reserve 9 bytes for length header by extending buffer length
	// This ensures we have space to write the header later
	if len(s.writer.buf) < MessagePackOffset {
		s.writer.buf = s.writer.buf[:MessagePackOffset]
	}
	s.writer.pos = MessagePackOffset

	// Create encoder with zero-copy writer
	enc := msgpack.NewEncoder(s.writer)
	enc.UseCompactInts(true)

	// Encode directly to buffer
	if err := enc.Encode(obj); err != nil {
		return nil, fmt.Errorf("zero-copy encode failed: %w", err)
	}

	// Calculate MessagePack data length
	msgpackLen := s.writer.pos - MessagePackOffset

	// Write 9-byte length header at the beginning
	// Format: 0xcd (long format marker) + 8-byte big-endian length
	s.writer.buf[0] = 0xcd
	binary.BigEndian.PutUint64(s.writer.buf[1:MessagePackOffset], uint64(msgpackLen))

	// Return buffer slice (zero additional allocation)
	return s.writer.Buffer(), nil
}

// DecodeZeroCopy deserializes object with zero-copy optimization.
// Reuses the input byte slice without copying.
func (s *ZeroCopySerializer) DecodeZeroCopy(data []byte, target interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("zero-copy decode: empty data")
	}

	// Check if data has 9-byte header
	if len(data) >= MessagePackOffset {
		// Read length header
		msgpackLen := int(binary.BigEndian.Uint64(data[1:MessagePackOffset]))

		// Validate length
		if MessagePackOffset+msgpackLen > len(data) {
			return fmt.Errorf("invalid length header: expected %d, got %d",
				MessagePackOffset+msgpackLen, len(data))
		}

		// Skip header, decode from MessagePackOffset
		data = data[MessagePackOffset : MessagePackOffset+msgpackLen]
	}

	// Create zero-copy reader
	reader := NewZeroCopyReader(data)
	s.reader = reader

	// Create decoder with zero-copy reader
	dec := msgpack.NewDecoder(reader)

	// Decode directly from byte slice
	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("zero-copy decode failed: %w", err)
	}

	return nil
}

// EncodeToBuffer encodes object to a pre-allocated buffer.
// This is the most efficient method when buffer pool is used.
func (s *ZeroCopySerializer) EncodeToBuffer(obj interface{}, buf *[]byte) error {
	// Reserve 9 bytes for header
	oldLen := len(*buf)
	*buf = append(*buf, make([]byte, MessagePackOffset+1024)...) // Initial 1KB after header
	*buf = (*buf)[:oldLen+MessagePackOffset]

	// Create encoder that appends to buffer
	bufWriter := &bufferWriter{buf: buf, start: oldLen + MessagePackOffset}
	enc := msgpack.NewEncoder(bufWriter)
	enc.UseCompactInts(true)

	// Encode
	if err := enc.Encode(obj); err != nil {
		return err
	}

	// Calculate MessagePack length
	msgpackLen := len(*buf) - (oldLen + MessagePackOffset)

	// Write header at the beginning
	// Use stack-allocated array to avoid heap allocation
	var header [9]byte
	header[0] = 0xcd
	binary.BigEndian.PutUint64(header[1:], uint64(msgpackLen))
	copy((*buf)[oldLen:oldLen+MessagePackOffset], header[:])

	return nil
}

// bufferWriter writes to a byte slice pointer
type bufferWriter struct {
	buf   *[]byte
	start int
}

func (w *bufferWriter) Write(p []byte) (int, error) {
	oldLen := len(*w.buf)
	needed := w.start + len(p) - oldLen

	if needed > 0 {
		// Grow buffer
		*w.buf = append(*w.buf, make([]byte, needed)...)
	}

	copy((*w.buf)[w.start:], p)
	w.start += len(p)

	return len(p), nil
}

// GetZeroCopyWriter returns a zero-copy writer from pool.
func GetZeroCopyWriter() *ZeroCopyWriter {
	return zeroCopyWriterPool.Get().(*ZeroCopyWriter)
}

// PutZeroCopyWriter returns a zero-copy writer to pool.
func PutZeroCopyWriter(w *ZeroCopyWriter) {
	w.Reset()
	zeroCopyWriterPool.Put(w)
}

// GetZeroCopyReader returns a zero-copy reader from pool.
func GetZeroCopyReader(data []byte) *ZeroCopyReader {
	r := zeroCopyReaderPool.Get().(*ZeroCopyReader)
	r.Reset(data)
	return r
}

// PutZeroCopyReader returns a zero-copy reader to pool.
func PutZeroCopyReader(r *ZeroCopyReader) {
	zeroCopyReaderPool.Put(r)
}

var (
	zeroCopyWriterPool = &sync.Pool{
		New: func() interface{} {
			return NewZeroCopyWriter(DefaultInitialBufferSize)
		},
	}

	zeroCopyReaderPool = &sync.Pool{
		New: func() interface{} {
			return &ZeroCopyReader{}
		},
	}
)

// Copyright (c) 2012-2020 Ugorji Nwoke. All rights reserved.
// Use of this source code is governed by a MIT license found in the LICENSE file.

package codec

import (
	"bufio"
	"errors"
	"io"
	"net/rpc"
)

var (
	errRpcIsClosed = errors.New("rpc - connection has been closed")
	errRpcNoConn   = errors.New("rpc - no connection")

	rpcSpaceArr = [1]byte{' '}
)

// Rpc provides a rpc Server or Client Codec for rpc communication.
type Rpc interface {
	ServerCodec(conn io.ReadWriteCloser, h Handle) rpc.ServerCodec
	ClientCodec(conn io.ReadWriteCloser, h Handle) rpc.ClientCodec
}

// RPCOptions holds options specific to rpc functionality
type RPCOptions struct {
	// RPCNoBuffer configures whether we attempt to buffer reads and writes during RPC calls.
	//
	// Set RPCNoBuffer=true to turn buffering off.
	// Buffering can still be done if buffered connections are passed in, or
	// buffering is configured on the handle.
	RPCNoBuffer bool
}

// rpcCodec defines the struct members and common methods.
type rpcCodec struct {
	c io.Closer
	r io.Reader
	w io.Writer
	f ioFlusher

	dec *Decoder
	enc *Encoder
	h   Handle

	cls atomicClsErr
}

func newRPCCodec(conn io.ReadWriteCloser, h Handle) rpcCodec {
	return newRPCCodec2(conn, conn, conn, h)
}

func newRPCCodec2(r io.Reader, w io.Writer, c io.Closer, h Handle) rpcCodec {
	bh := h.getBasicHandle()
	// if the writer can flush, ensure we leverage it, else
	// we may hang waiting on read if write isn't flushed.
	// var f ioFlusher
	f, ok := w.(ioFlusher)
	if !bh.RPCNoBuffer {
		if bh.WriterBufferSize <= 0 {
			if !ok { // a flusher means there's already a buffer
				bw := bufio.NewWriter(w)
				f, w = bw, bw
			}
		}
		if bh.ReaderBufferSize <= 0 {
			if _, ok = w.(ioBuffered); !ok {
				r = bufio.NewReader(r)
			}
		}
	}
	return rpcCodec{
		c:   c,
		w:   w,
		r:   r,
		f:   f,
		h:   h,
		enc: NewEncoder(w, h),
		dec: NewDecoder(r, h),
	}
}

func (c *rpcCodec) write(obj ...interface{}) (err error) {
	err = c.ready()
	if err != nil {
		return
	}
	if c.f != nil {
		defer func() {
			flushErr := c.f.Flush()
			if flushErr != nil && err == nil {
				err = flushErr
			}
		}()
	}

	for _, o := range obj {
		err = c.enc.Encode(o)
		if err != nil {
			return
		}
		// defensive: ensure a space is always written after each encoding,
		// in case the value was a number, and encoding a value right after
		// without a space will lead to invalid output.
		if c.h.isJson() {
			_, err = c.w.Write(rpcSpaceArr[:])
			if err != nil {
				return
			}
		}
	}
	return
}

func (c *rpcCodec) read(obj interface{}) (err error) {
	err = c.ready()
	if err == nil {
		//If nil is passed in, we should read and discard
		if obj == nil {
			// return c.dec.Decode(&obj)
			err = c.dec.swallowErr()
		} else {
			err = c.dec.Decode(obj)
		}
	}
	return
}

func (c *rpcCodec) Close() (err error) {
	if c.c != nil {
		cls := c.cls.load()
		if !cls.closed {
			cls.err = c.c.Close()
			cls.closed = true
			c.cls.store(cls)
		}
		err = cls.err
	}
	return
}

func (c *rpcCodec) ready() (err error) {
	if c.c == nil {
		err = errRpcNoConn
	} else {
		cls := c.cls.load()
		if cls.closed {
			if err = cls.err; err == nil {
				err = errRpcIsClosed
			}
		}
	}
	return
}

func (c *rpcCodec) ReadResponseBody(body interface{}) error {
	return c.read(body)
}

// -------------------------------------

type goRpcCodec struct {
	rpcCodec
}

func (c *goRpcCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	return c.write(r, body)
}

func (c *goRpcCodec) WriteResponse(r *rpc.Response, body interface{}) error {
	return c.write(r, body)
}

func (c *goRpcCodec) ReadResponseHeader(r *rpc.Response) error {
	return c.read(r)
}

func (c *goRpcCodec) ReadRequestHeader(r *rpc.Request) error {
	return c.read(r)
}

func (c *goRpcCodec) ReadRequestBody(body interface{}) error {
	return c.read(body)
}

// -------------------------------------

// goRpc is the implementation of Rpc that uses the communication protocol
// as defined in net/rpc package.
type goRpc struct{}

// GoRpc implements Rpc using the communication protocol defined in net/rpc package.
//
// Note: network connection (from net.Dial, of type io.ReadWriteCloser) is not buffered.
//
// For performance, you should configure WriterBufferSize and ReaderBufferSize on the handle.
// This ensures we use an adequate buffer during reading and writing.
// If not configured, we will internally initialize and use a buffer during reads and writes.
// This can be turned off via the RPCNoBuffer option on the Handle.
//
//	var handle codec.JsonHandle
//	handle.RPCNoBuffer = true // turns off attempt by rpc module to initialize a buffer
//
// Example 1: one way of configuring buffering explicitly:
//
//	var handle codec.JsonHandle // codec handle
//	handle.ReaderBufferSize = 1024
//	handle.WriterBufferSize = 1024
//	var conn io.ReadWriteCloser // connection got from a socket
//	var serverCodec = GoRpc.ServerCodec(conn, handle)
//	var clientCodec = GoRpc.ClientCodec(conn, handle)
//
// Example 2: you can also explicitly create a buffered connection yourself,
// and not worry about configuring the buffer sizes in the Handle.
//
//	var handle codec.Handle     // codec handle
//	var conn io.ReadWriteCloser // connection got from a socket
//	var bufconn = struct {      // bufconn here is a buffered io.ReadWriteCloser
//	    io.Closer
//	    *bufio.Reader
//	    *bufio.Writer
//	}{conn, bufio.NewReader(conn), bufio.NewWriter(conn)}
//	var serverCodec = GoRpc.ServerCodec(bufconn, handle)
//	var clientCodec = GoRpc.ClientCodec(bufconn, handle)
var GoRpc goRpc

func (x goRpc) ServerCodec(conn io.ReadWriteCloser, h Handle) rpc.ServerCodec {
	return &goRpcCodec{newRPCCodec(conn, h)}
}

func (x goRpc) ClientCodec(conn io.ReadWriteCloser, h Handle) rpc.ClientCodec {
	return &goRpcCodec{newRPCCodec(conn, h)}
}

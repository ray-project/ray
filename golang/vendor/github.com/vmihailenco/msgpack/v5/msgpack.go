package msgpack

import "fmt"

type Marshaler interface {
	MarshalMsgpack() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalMsgpack([]byte) error
}

type CustomEncoder interface {
	EncodeMsgpack(*Encoder) error
}

type CustomDecoder interface {
	DecodeMsgpack(*Decoder) error
}

//------------------------------------------------------------------------------

type RawMessage []byte

var (
	_ CustomEncoder = (RawMessage)(nil)
	_ CustomDecoder = (*RawMessage)(nil)
)

func (m RawMessage) EncodeMsgpack(enc *Encoder) error {
	return enc.write(m)
}

func (m *RawMessage) DecodeMsgpack(dec *Decoder) error {
	msg, err := dec.DecodeRaw()
	if err != nil {
		return err
	}
	*m = msg
	return nil
}

//------------------------------------------------------------------------------

type unexpectedCodeError struct {
	code byte
	hint string
}

func (err unexpectedCodeError) Error() string {
	return fmt.Sprintf("msgpack: unexpected code=%x decoding %s", err.code, err.hint)
}

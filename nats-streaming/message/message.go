// Package message defines the messages used for communication between
// services. Any producer or consumer of messages will do so using the messages
// defined in this package.
package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/chadit/MessageQueueExamples/nats-streaming/util/gzip"
)

var (
	// ErrNilMsg indicates a nil Msg.
	ErrNilMsg = errors.New("me: message.Msg cannot be nil")
	// ErrNilEncoded indicates a nil encoded argument.
	ErrNilEncoded = errors.New("me: encoded arguments cannot be nil")
)

// Payloader represents an object which will be treated as message payload.
type Payloader interface{}

// Host provides process related information.
type Host struct {
	Component string `json:"component,omitempty"` // service requesting/publishing message
}

// Msg represents information transmitted via the queue.
type Msg struct {
	SessionID      string          `json:"sessionId,omitempty"`      // session id
	TransactionID  string          `json:"transactionId,omitempty"`  // transaction id
	CreatedAt      time.Time       `json:"createdAt,omitempty"`      // timestamp of message creation
	Text           string          `json:"text,omitempty"`           // message text
	Subject        string          `json:"subject,omitempty"`        // message subject
	Reply          string          `json:"reply,omitempty"`          // message reply
	PayloadKey     string          `json:"payloadKey,omitempty"`     // payload key
	PayloadType    string          `json:"payloadType,omitempty"`    // payload type represents the a Go-syntax string representation of the type of the value.
	Host           *Host           `json:"host,omitempty"`           // includes information about service requesting/publishing message
	Payload        Payloader       `json:"-"`                        // Payload represents a generic type used by clients for payload like types.
	PayloadNoGZip  bool            `json:"PayloadNoGZip,omitempty"`  // PayloadNoGZip is the drivers for decision to compress/uncompress Payload.
	DocumentSchema json.RawMessage `json:"documentSchema,omitempty"` // Represents a JSON schema of a given payload.
}

// ClearPayloads sets payload fields to their zero values.
func (m *Msg) ClearPayloads() {
	m.Payload = nil
}

// msg is used at the time of marshalling/unmarshalling of the Msg type
type msg struct {
	alias
	GZipPayload []byte `json:"gzipp,omitempty"` // compressed generic payload
}

type (
	// alias is used to disconnect struct methods and prevent potential loop.
	alias Msg
	// Handler is a function that expects to be called in response to messages being retrieved from the queue.
	Handler func(*Msg)
)

// MarshalJSON populates the CreatedAt and Version fields, marshals the message
// into JSON, and compresses the payloads using gzip.
func (m *Msg) MarshalJSON() ([]byte, error) {
	if m == nil {
		return nil, ErrNilMsg
	}

	v := msg{alias: alias(*m)}

	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}

	if v.Payload != nil {
		var err error
		v.PayloadType = fmt.Sprintf("%T", v.Payload)
		if !v.PayloadNoGZip {
			if v.GZipPayload, err = compress(v.Payload); err != nil {
				return nil, err
			}
			v.Payload = nil
		}
	} else {
		v.PayloadType = ""
	}

	return json.Marshal(v)
}

// UnmarshalJSON unmarshals the given byte slice into a message, and
// decompresses the payload.
func (m *Msg) UnmarshalJSON(b []byte) error {
	if m == nil {
		return ErrNilMsg
	}

	if len(b) == 0 {
		return ErrNilEncoded
	}

	v := msg{alias: alias(*m)}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	if v.GZipPayload != nil {
		if !v.PayloadNoGZip {
			var err error
			if v.Payload, err = gzip.DeflateGZip(v.GZipPayload); err != nil {
				return err
			}
		}
	}

	*m = Msg(v.alias)

	return nil
}

// HasPayload identifies the existence at least one payload element.
func (m Msg) HasPayload() bool {
	return m.Payload != nil
}

// Decode a a helper function used to decode a type previously decoded into an interface.
// It attemps to decode message's payload into the expected type's instance.
// This function could be improved since it uses an extra marshalling step in order to solve the original JSON encoded value.
func (m *Msg) Decode(p Payloader) error {
	if m == nil || m.PayloadType == "" {
		return nil
	}

	if b, ok := m.Payload.([]byte); ok {
		return json.Unmarshal(b, p)
	}

	b, err := json.Marshal(m.Payload)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, p)
}

// compress is a helper function specific to Message behaviour.
func compress(v Payloader) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return gzip.InflateGZip(b)
	}

	return gzip.Compress(v)
}

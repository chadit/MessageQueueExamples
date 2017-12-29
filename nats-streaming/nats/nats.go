package nats

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	message "github.com/chadit/MessageQueueExamples/nats-streaming/message"
	crypto1 "github.com/chadit/MessageQueueExamples/nats-streaming/util/crypto"
	stan "github.com/nats-io/go-nats-streaming"
	uuid "github.com/satori/uuid"
)

// rawHandler is a function that expects to be called in response to stream response
type rawHandler func([]byte)

var (
	// ErrNoTimeout indicates that Timeout config param wasn't provided.
	ErrNoTimeout = errors.New("na: Timeout config param wasn't provided")
	// ErrNoURLList indicates that URLList config param wasn't provided.
	ErrNoURLList = errors.New("na: URLList config param wasn't provided")
	// ErrNotInitialized indicates that initialization hasn't occurred
	ErrNotInitialized = errors.New("na: initialization hasn't occurred")
	// ErrNoCrypto indicates that Crypto config param wasn't provided.
	ErrNoCrypto = errors.New("na: Crypto config param wasn't provided")
	// ErrNoClusterID indicates a missing Nats streaming cluster ID.
	ErrNoClusterID = errors.New("na: missing cluster id")
	// ErrNoClientID indicates a missing The NATS Streaming client ID to connect with.
	ErrNoClientID = errors.New("na: missing client id")
	// ErrClientIDNotUnique clientID must be unique per connection
	ErrClientIDNotUnique = errors.New("na: client id has already been registered")
	// ErrNoHandler indicates a missing handler.
	ErrNoHandler = errors.New("na: missing handler")
	// ErrTimeout is an alias for stan.ErrTimeout.
	ErrTimeout = stan.ErrTimeout
	// ErrInvalidSubject indicates that the subject invalid.
	ErrInvalidSubject = errors.New("na: invalid subject")
	// ErrRequestTimeout indicates that a request call timed out
	ErrRequestTimeout = errors.New("na: request timeout")
	// ErrConnectionClosed indicates that the nats connection was closed
	ErrConnectionClosed = errors.New("na: connection was closed")
)

// ConnHandler is used for asynchronous events such as
// disconnected and closed connections.
type ConnHandler func(*T)

// T holds all properties necessary for pub-sub queue message transmissions.
type T struct {
	ClusterID     *string        // id of the node
	ClientID      *string        //clientID can contain only alphanumeric and `-` or `_` characters.
	Crypto        *string        // key used by the conrypto package
	Timeout       *time.Duration // maximum amount of time to wait before aborting a request
	URLList       *string        // NATS connection URL
	connection    stan.Conn      // connection to the NATS server
	ClosedHandler ConnHandler    // Handler for closed connection events
	crypto        *crypto1.T     // handles cryptography concerns
	QueueName     string         // NATS queue name, which ensures at-most-once-per-service message delivery
	initialized   bool           // indicates whether initialization has occurred

}

// Subscription represents a single subscription
type Subscription struct {
	Sequence    uint64
	Subject     string
	Reply       string
	DurableName string
	QueueName   string
}

// Initialize initializes the instance.
// This method is meant to be called exactly once.
// Calling other methods without first calling this method will produce an error.
func (t *T) Initialize() error {
	if t.initialized {
		return nil
	}

	if t.ClusterID == nil {
		return ErrNoClusterID
	}

	if t.ClientID == nil {
		return ErrNoClientID
	}

	if t.Crypto == nil {
		return ErrNoCrypto
	}

	if t.Timeout == nil {
		return ErrNoTimeout
	}

	if t.URLList == nil {
		return ErrNoURLList
	}

	var err error
	c := crypto1.Config{Key: *t.Crypto}
	if t.crypto, err = crypto1.New(c); err != nil {
		return err
	}
	nu := strings.Split(*t.URLList, ",")
	t.connection, err = stan.Connect(*t.ClusterID, *t.ClientID, stan.NatsURL(nu[0]), stan.ConnectWait(*t.Timeout))
	if err != nil {
		return t.errorHandler("init", err)
	}

	t.QueueName = os.Args[0]
	if i := strings.LastIndex(t.QueueName, "/"); i != -1 {
		t.QueueName = string(t.QueueName[i+1:])
	}

	t.initialized = true
	return nil
}

// Request - serial calls - when the request is made, the thread will wait for a response or timeout
// while nats supports this, nats-streaming does not support one and done subscriptions, this adds a layer to support it.
func (t *T) Request(m, p *message.Msg) error {
	if !t.initialized {
		return ErrNotInitialized
	}

	var (
		err error
		wg  sync.WaitGroup
	)
	wg.Add(1)
	m.Reply = uuid.NewV4().String()
	var completed int32 = 1

	h := func(mr []byte) {
		atomic.AddInt32(&completed, -1)
		t.Decode(mr, p)
		wg.Done()
	}

	sHandler, err := t.rawSubscribe(Subscription{Subject: m.Reply, QueueName: "request"}, h)
	if err != nil {
		return t.errorHandler(m.Reply, err)
	}

	err = t.Publish(m)
	if err != nil {
		t.CloseHandler(sHandler)
		return err
	}

	// timeout handler

	go func(completed *int32) {
		td := time.Duration(time.Second * 300)
		if t.Timeout != nil {
			td = *t.Timeout
		}

		<-time.After(td)
		if completed != nil && *completed != 0 {
			atomic.AddInt32(completed, -1)
			err = ErrRequestTimeout
			wg.Done()
		}
	}(&completed)

	wg.Wait()
	t.CloseHandler(sHandler)
	return err
}

// Publish puts a given message into the queue.
func (t *T) Publish(m *message.Msg) error {
	if !t.initialized {
		return ErrNotInitialized
	}

	subj := filterSubject(m.Subject)
	if err := t.safeSubject(subj); err != nil {
		return err
	}

	data, err := t.Encode(m)
	if err != nil {
		return err
	}
	return t.errorHandler(subj, t.connection.Publish(subj, data))
}

// Subscribe registers a handler to be called in response to messages being received from the queue.
func (t *T) Subscribe(sub Subscription, h ...message.Handler) (stan.Subscription, error) {
	if !t.initialized {
		return nil, ErrNotInitialized
	}

	if sub.Subject == "" {
		return nil, ErrInvalidSubject
	}

	if len(h) < 1 {
		return nil, ErrNoHandler
	}

	process := func(ms *stan.Msg) {
		if sub.DurableName != "" {
			ms.Ack()
		}
		m := new(message.Msg)
		err := t.Decode(ms.Data, m)
		if err != nil {
			m.Text = fmt.Sprintf("sub error decoding %s : %v", ms.Subject, err)
		}

		for i := range h {
			h[i](m)
		}
	}

	return t.subscribe(sub, process)
}

// rawHandler registers a handler to be called in response to messages being received from the queue.
func (t *T) rawSubscribe(sub Subscription, h ...rawHandler) (stan.Subscription, error) {
	if !t.initialized {
		return nil, ErrNotInitialized
	}

	if sub.Subject == "" {
		return nil, ErrInvalidSubject
	}

	if len(h) < 1 {
		return nil, ErrNoHandler
	}

	process := func(ms *stan.Msg) {
		if sub.DurableName != "" {
			ms.Ack()
		}
		for i := range h {
			h[i](ms.Data)
		}
	}

	return t.subscribe(sub, process)
}

func (t *T) subscribe(sub Subscription, p stan.MsgHandler) (stan.Subscription, error) {

	subj := filterSubject(sub.Subject)
	if err := t.safeSubject(subj); err != nil {
		return nil, err
	}

	var (
		err        error
		subHandler stan.Subscription
		options    []stan.SubscriptionOption
	)

	if sub.DurableName != "" {
		options = append(options, stan.DurableName(sub.DurableName))
		options = append(options, stan.SetManualAckMode())
	}

	if sub.Sequence != 0 {
		options = append(options, stan.StartAtSequence(sub.Sequence))
	}

	if sub.QueueName == "" {
		subHandler, err = t.connection.Subscribe(subj, p, options...)
	} else {
		subHandler, err = t.connection.QueueSubscribe(subj, sub.QueueName, p, options...)
	}

	return subHandler, t.errorHandler(subj, err)
}

// NATS protocol conventions define subject names,
// including reply subject (INBOX) names, as case-sensitive and must be
// non-empty alphanumeric strings with no embedded whitespace,
// and optionally token-delimited using the dot character.
// Invalid subject names can lead to a NATS client faulted state.
func (t T) safeSubject(s string) error {
	r, err := regexp.Compile(`^([\w\/\-\(\)]+|\*|\>)$`)
	if err != nil {
		return err
	}
	tokens := strings.Split(s, ".")
	for _, t := range tokens {
		all := r.FindAllString(t, -1)
		if len(all) != 1 {
			return ErrInvalidSubject
		}
	}

	return nil
}

// NATS protocol conventions does not allow subjects to have certain characters.
// There is a need to allow ! in naming convensions which is invalid for NATS
// this filter to parse characters out of subjects.
func filterSubject(s string) string {
	return strings.Replace(s, "!", "", -1)
}

// Encode allows for messages to be serialized for transmission via nats
// Encode also AES-encrypts the message data for security
func (t *T) Encode(m *message.Msg) ([]byte, error) {
	b, err := m.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return t.crypto.Encrypt(b)
}

// Decode allows messages to be de-serialized from the nats transmission
// Decode also AES-descrypts the message data transmitted across the network
func (t *T) Decode(b []byte, p *message.Msg) error {
	b, err := t.crypto.Decrypt(b)
	if err != nil {
		return err
	}

	err = p.UnmarshalJSON(b)
	return err
}

// Close - closes the connection for nats-streaming
func (t *T) Close() error {
	return t.connection.Close()
}

func (t *T) errorHandler(subj string, err error) error {
	if err == nil {
		return nil
	}
	e := err.Error()
	switch e {
	case "stan: connection closed":
		t.ClosedHandler(t)
		return ErrConnectionClosed
	case "stan: clientID already registered":
		return ErrClientIDNotUnique
	}
	return fmt.Errorf("subject %s errored %v", subj, err)
}

// CloseHandler will close out subscription handlers when a request action is completed (these are typically one and done actions)
func (t *T) CloseHandler(sHandlers ...stan.Subscription) {
	for _, sHandler := range sHandlers {
		sHandler.Unsubscribe()
		sHandler.Close()
	}
}

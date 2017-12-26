package nats_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	message "github.com/chadit/MessageQueueExamples/nats-streaming/message"
	nats1 "github.com/chadit/MessageQueueExamples/nats-streaming/nats"
	"github.com/satori/uuid"
	//gouuid "github.com/satori/go.uuid"
)

// mockPayload is used as a mock message.Payload for testing.
type mockPayload struct {
	Providers []mockProvider
}

// mockProvider is mock field within the mockPayload struct.
type mockProvider struct {
	Value string
}

var (
	// instance is an instance of the cache that will be tested.
	// Without it, each test would have to handle the creation of their own instance.
	instance  *nats1.T
	clusterID *string
	clientID  *string
	urlList   *string
	timeout   *time.Duration
	crypto    *string
)

func init() {
	clusterID = flag.String("cluster", "test-cluster", "The NATS Streaming cluster ID")
	urlList = flag.String("nu", "nats://172.16.0.2:4222", "NATS URLs")
	timeout = flag.Duration("nt", time.Second, "NATS timeout")
	crypto = flag.String("nk", "A1B2c3D4e5F63c7u", "NATS crypto key")
	flag.Parse()

	cID := os.Args[0]
	if i := strings.LastIndex(cID, "/"); i != -1 {
		cID = strings.Replace(string(cID[i+1:]), ".", "_", -1)
	}
	clientID = &cID

	instance = &nats1.T{
		ClusterID: clusterID,
		ClientID:  clientID,
		URLList:   urlList,
		Timeout:   timeout,
		Crypto:    crypto,
	}

	if err := instance.Initialize(); err != nil {
		log.Fatalf("Error while creating nats connection: %v", err)
	}
}

func TestInitialize(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	nt, _ := time.ParseDuration("1s")
	ucliID := "nats_test_init"
	urls := *instance.URLList
	tests := []struct {
		clusterID *string
		clientID  *string
		timeout   *time.Duration
		crypto    *string
		urls      *string
		error     error
	}{
		// should accept all parameters
		{clusterID: instance.ClusterID, clientID: &ucliID, crypto: new(string), timeout: &nt, urls: &urls},
		// missing nats streaming clusterID
		{clientID: instance.ClientID, crypto: new(string), timeout: &nt, urls: &urls, error: nats1.ErrNoClusterID},
		// missing nats streaming clientID
		{clusterID: instance.ClusterID, crypto: new(string), timeout: &nt, urls: &urls, error: nats1.ErrNoClientID},
		// should require Crypto
		{clusterID: instance.ClusterID, clientID: instance.ClientID, timeout: new(time.Duration), urls: &urls, error: nats1.ErrNoCrypto},
		// should require Timeout
		{clusterID: instance.ClusterID, clientID: instance.ClientID, crypto: new(string), urls: &urls, error: nats1.ErrNoTimeout},
		// should require URLList
		{clusterID: instance.ClusterID, clientID: instance.ClientID, crypto: new(string), timeout: new(time.Duration), error: nats1.ErrNoURLList},
		// client id should be unique for connections
		{clusterID: instance.ClusterID, clientID: instance.ClientID, crypto: new(string), timeout: &nt, urls: &urls, error: nats1.ErrClientIDNotUnique},
	}

	for index, test := range tests {
		o := nats1.T{
			ClusterID: test.clusterID,
			ClientID:  test.clientID,
			Crypto:    test.crypto,
			Timeout:   test.timeout,
			URLList:   test.urls,
		}

		if err := o.Initialize(); err != test.error {
			t.Fatalf("Test %d produced error: (%v), not (%v)", index, err, test.error)
		}
	}
}

func TestPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	tests := []struct {
		cacheKey string
	}{
		{cacheKey: "KEY-X"},
		{cacheKey: "KEY-Y"},
	}

	var (
		subject  = uuid.NewV4().String()
		received = make(chan *message.Msg, 1)
	)

	subHandler, err := instance.Subscribe(nats1.Subscription{Subject: subject}, func(m *message.Msg) {
		received <- m
	})

	if err != nil {
		t.Fatalf("Error while subscribing to queue: %v", err)
	}

	for index, test := range tests {
		m := message.Msg{
			PayloadKey: test.cacheKey,
			Payload:    &mockPayload{Providers: []mockProvider{{Value: "mock"}}},
			Subject:    subject,
		}

		if err := instance.Publish(&m); err != nil {
			t.Fatalf("test %d produced Publish error: %v", index, err)
		}

		r := <-received
		if r.PayloadKey != test.cacheKey {
			t.Fatalf("test %d published message: %v not %v", index, r.PayloadKey, test.cacheKey)
		}
	}

	subHandler.Unsubscribe()
	subHandler.Close()
}

func TestSubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	var notified int32
	h := func(m *message.Msg) {
		atomic.AddInt32(&notified, 1)
	}

	tests := []struct {
		error    error
		notified int32
		subj     string
		handlers []message.Handler
	}{
		// It should guarantee at-most-once delivery per queue.
		{subj: "TEST-A", notified: 3, handlers: []message.Handler{h}},
		// Accepts multiple handlers.
		{subj: "TEST-B", notified: 4, handlers: []message.Handler{h, h}},
		// It should error if no handlers provided.
		{subj: "TEST-C", error: nats1.ErrNoHandler},
		// It should error if invalid subject is used.
		{subj: "TEST C", handlers: []message.Handler{h}, error: nats1.ErrInvalidSubject},
		{subj: "TEST!A", notified: 3, handlers: []message.Handler{h}},
	}

	for index, test := range tests {
		s := test.subj
		sub1, _ := instance.Subscribe(nats1.Subscription{Subject: s}, h)
		sub2, _ := instance.Subscribe(nats1.Subscription{Subject: s}, h)
		sub3, _ := instance.Subscribe(nats1.Subscription{Subject: s, QueueName: instance.QueueName}, test.handlers...)
		sub4, err := instance.Subscribe(nats1.Subscription{Subject: s, QueueName: instance.QueueName}, test.handlers...)
		if err != test.error {
			t.Fatalf("test %d produced QueueSubscribe error: (%v) not (%v)", index, err, test.error)
		}

		if test.error != nil {
			continue
		}

		m := message.Msg{Subject: s}
		atomic.StoreInt32(&notified, 0)
		if err := instance.Publish(&m); err != nil {
			t.Fatalf("test %d coudn't publish message: %v", index, err)
		}

		<-time.After(time.Second / 4)
		if n := atomic.LoadInt32(&notified); n != test.notified {
			t.Fatalf("test %d notified %d handlers, not %d", index, n, test.notified)
		}
		instance.CloseHandler(sub1, sub2, sub3, sub4)
	}
}

func TestRequestForward(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	tests := []struct {
		payload message.Payloader
		err     error
	}{
		{payload: &mockPayload{Providers: []mockProvider{{Value: "mock"}}}},
		{payload: &mockPayload{}},
	}

	var (
		transactionID = uuid.NewV4().String()
		forwarderSub  = uuid.NewV4().String()
		replierSub    = uuid.NewV4().String()
	)

	instance.Subscribe(nats1.Subscription{Subject: forwarderSub}, func(m *message.Msg) {
		m.Subject = replierSub
		m.Payload = m.Payload
		instance.Publish(m)
	})

	instance.Subscribe(nats1.Subscription{Subject: replierSub}, func(m *message.Msg) {
		m.Subject = m.Reply
		instance.Publish(m)
	})

	for index, test := range tests {
		m := message.Msg{
			TransactionID: transactionID,
			Subject:       forwarderSub,
			Payload:       test.payload,
			Text:          fmt.Sprintf("test-%d", index),
		}

		var r message.Msg
		if err := instance.Request(&m, &r); err != test.err {
			t.Fatalf("test %d produced Request error: (%v) not (%v)", index, err, test.err)
		}

		if x, y := m.TransactionID, transactionID; x != y {
			t.Fatalf("test %d sent TransactionID: (%v) not (%v)", index, x, y)
		}
		if test.err != nil {
			continue
		}

		b, _ := json.Marshal(test.payload)
		if fmt.Sprint(r.Payload) != fmt.Sprint(b) {
			t.Fatalf("test %d received payload: %s not %s", index, r.Payload, b)
		}
	}
}

// func TestSafeSubject(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("Skipping integration test")
// 	}

// 	tests := []struct {
// 		subj string
// 		err  error
// 	}{
// 		{subj: ">", err: nil},
// 		{subj: "foo", err: nil},
// 		{subj: "foo.bar", err: nil},
// 		{subj: "foo.bar.baz", err: nil},
// 		{subj: "foo.>", err: nil},
// 		{subj: "foo.*.baz", err: nil},
// 		{subj: "foo.*.>", err: nil},
// 		{subj: "foo.bar/baz.foo.foo-bar", err: nil},
// 		{subj: "foo.bar/baz.(foo).foo-bar", err: nil},
// 		{subj: "foo-bar", err: nil},
// 		{subj: "-", err: nil},
// 		{subj: "", err: nats1.ErrInvalidSubject},
// 		{subj: " ", err: nats1.ErrInvalidSubject},
// 		{subj: "_", err: nats1.ErrInvalidSubject},
// 		{subj: "foo.", err: nats1.ErrInvalidSubject},
// 		{subj: "foo..bar", err: nats1.ErrInvalidSubject},
// 		{subj: "foo bar", err: nats1.ErrInvalidSubject},
// 		{subj: "foo  bar", err: nats1.ErrInvalidSubject},
// 		{subj: "foo.**", err: nats1.ErrInvalidSubject},
// 		{subj: "foo.*bar", err: nats1.ErrInvalidSubject},
// 		{subj: "foo.*bar.", err: nats1.ErrInvalidSubject},
// 		{subj: "foo.*bar.>", err: nats1.ErrInvalidSubject},
// 		{subj: "foo.bar>", err: nats1.ErrInvalidSubject},
// 	}
// 	m := message.Msg{
// 		TransactionID: uuid.NewV4().String(),
// 	}
// 	for index, test := range tests {
// 		m.Subject = test.subj
// 		if err := instance.Publish(&m); err != nil {
// 			if err != test.err {
// 				t.Fatalf("Test %d produced error %v not %v", index, err, test.err)
// 			}
// 		}
// 	}
// }

func TestClosedConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	td := time.Duration(time.Second * 30)
	closeClientID := "close-test"
	var isClosed bool
	n := &nats1.T{
		ClusterID: clusterID,
		ClientID:  &closeClientID,
		URLList:   urlList,
		Timeout:   &td,
		Crypto:    crypto,
		ClosedHandler: func(t *nats1.T) {
			isClosed = true
		},
	}
	if err := n.Initialize(); err != nil {
		t.Fatalf("Error during NATS setup: %v", err)
	}
	n.Close()
	m := message.Msg{
		Subject: uuid.NewV4().String(),
	}

	if _, err := n.Subscribe(nats1.Subscription{Subject: m.Subject}, func(m *message.Msg) {}); err != nats1.ErrConnectionClosed {
		t.Fatalf("test produced subscribe error: %v", err)
	}

	if err := n.Publish(&m); err != nats1.ErrConnectionClosed {
		t.Fatalf("test produced Publish error: %v", err)
	}

	<-time.After(time.Second / 4)
	if !isClosed {
		t.Fatal("Caller was not notified the connection is closed")
	}
}

func TestDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	td := time.Duration(time.Second * 30)
	closeClientID := "durability-test"
	n := &nats1.T{
		ClusterID: clusterID,
		ClientID:  &closeClientID,
		URLList:   urlList,
		Timeout:   &td,
		Crypto:    crypto,
	}
	if err := n.Initialize(); err != nil {
		t.Fatalf("Error during NATS setup: %v", err)
	}
	var (
		sub      = uuid.NewV4().String()
		msgCount = 4
		msgs     []*message.Msg
	)

	publishDoneCh := make(chan bool, 1)

	sh, _ := n.Subscribe(nats1.Subscription{Subject: sub, DurableName: "test-dur"}, func(m *message.Msg) {
		msgs = append(msgs, m)
		<-publishDoneCh
	})

	for i := 0; i < msgCount; i++ {
		m := message.Msg{Subject: sub, Text: fmt.Sprintf("message%d", i)}
		if err := n.Publish(&m); err != nil {
			t.Fatalf("test produced Publishing error: %v", err)
		}
	}

	// simulate service going down while processing
	sh.Close()
	publishDoneCh <- true
	// re-connect the subscription
	sh2, _ := n.Subscribe(nats1.Subscription{Subject: sub, DurableName: "test-dur"}, func(m *message.Msg) {
		msgs = append(msgs, m)
	})

	<-time.After(time.Second * 1)
	if len(msgs) != msgCount {
		t.Fatalf("test did not get back the expected message count, expected %d, but got %d", msgCount, len(msgs))

	}

	n.CloseHandler(sh, sh2)
}

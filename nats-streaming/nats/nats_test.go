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

	"github.com/nats-io/go-nats-streaming"

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
	instance *nats1.T
	clientID *string
)

func init() {
	cID := os.Args[0]
	if i := strings.LastIndex(cID, "/"); i != -1 {
		cID = strings.Replace(string(cID[i+1:]), ".", "_", -1)
	}
	clientID = &cID

	instance = &nats1.T{
		ClusterID: flag.String("cluster", "test-cluster", "The NATS Streaming cluster ID"),
		ClientID:  clientID,
		URLList:   flag.String("nu", "nats://172.16.0.2:4222", "NATS URLs"),
		Timeout:   flag.Duration("nt", time.Second, "NATS timeout"),
		Crypto:    flag.String("nk", "A1B2c3D4e5F63c7u", "NATS crypto key"),
	}

	flag.Parse()
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
		closeSub(sub1, sub2, sub3, sub4)
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

func closeSub(sHandlers ...stan.Subscription) {
	for _, sHandler := range sHandlers {
		sHandler.Unsubscribe()
		sHandler.Close()
	}

}

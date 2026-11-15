package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type orderMsg struct {
	subject string
	data    []byte
	headers map[string][]string
}

var ordersBefore = []orderMsg{
	{"orders.eu.created", []byte(`{"id":1,"region":"eu","amount":10.5}`), map[string][]string{"X-Tenant": {"acme"}}},
	{"orders.us.created", []byte(`{"id":2,"region":"us","amount":20}`), map[string][]string{"X-Tenant": {"globex"}}},
	{"orders.eu.shipped", []byte(`{"id":1}`), nil},
	{"orders.us.created", []byte(`{"id":3,"region":"us","amount":7.25}`), nil},
	{"orders.eu.created", []byte{0xff, 0xfe, 0x00, 0x01}, nil},
}

var ordersAfter = []orderMsg{
	{"orders.us.shipped", []byte(`{"id":2}`), map[string][]string{"X-Tenant": {"globex"}, "Nats-Msg-Id": {"ord-6"}, "X-Trace": {"a", "b"}}},
	{"orders.eu.created", []byte(`{"id":4,"region":"eu","amount":99}`), nil},
	{"orders.eu.shipped", []byte(`{"id":4}`), nil},
	{"orders.apac.created", []byte(`{"id":5,"region":"apac","amount":1}`), nil},
	{"orders.us.created", []byte{}, nil},
}

// BulkMessages is the size of the BULK stream.
const BulkMessages = 25000

func pubOrders(ctx context.Context, js jetstream.JetStream, msgs []orderMsg) error {
	for _, m := range msgs {
		msg := natsio.NewMsg(m.subject)
		msg.Data = m.data
		for k, vs := range m.headers {
			for _, v := range vs {
				msg.Header.Add(k, v)
			}
		}
		if _, err := js.PublishMsg(ctx, msg); err != nil {
			return fmt.Errorf("publish %s: %w", m.subject, err)
		}
	}
	return nil
}

// publish creates every stream, consumer, bucket and object, and returns the
// time boundary between ORDERS seq 5 and seq 6.
func publish(nc *natsio.Conn) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		return time.Time{}, err
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "ORDERS", Description: "Order lifecycle events", Subjects: []string{"orders.>"},
		Storage: jetstream.FileStorage, Duplicates: time.Minute, AllowDirect: true,
	}); err != nil {
		return time.Time{}, err
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "EVENTS", Subjects: []string{"events.*", "audit.>"}, Storage: jetstream.MemoryStorage, MaxMsgs: 1000,
	}); err != nil {
		return time.Time{}, err
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "BULK", Subjects: []string{"bulk.*"}, Storage: jetstream.FileStorage,
	}); err != nil {
		return time.Time{}, err
	}
	if err := pubOrders(ctx, js, ordersBefore); err != nil {
		return time.Time{}, err
	}
	time.Sleep(50 * time.Millisecond)
	boundary := time.Now()
	time.Sleep(50 * time.Millisecond)
	if err := pubOrders(ctx, js, ordersAfter); err != nil {
		return time.Time{}, err
	}
	for _, s := range []string{"events.login", "events.logout", "audit.user.created", "events.login"} {
		if _, err := js.Publish(ctx, s, []byte(s)); err != nil {
			return time.Time{}, err
		}
	}
	for i := 1; i <= BulkMessages; i++ {
		if _, err := js.PublishAsync(fmt.Sprintf("bulk.%d", i%7), []byte(fmt.Sprintf("row-%05d", i))); err != nil {
			return time.Time{}, err
		}
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	}

	// Durable consumers with known delivery state.
	shipper, err := js.CreateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable: "shipper", Description: "Ships orders", FilterSubject: "orders.*.shipped",
		AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 5,
	})
	if err != nil {
		return time.Time{}, err
	}
	batch, err := shipper.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		return time.Time{}, err
	}
	for m := range batch.Messages() {
		if err := m.DoubleAck(ctx); err != nil {
			return time.Time{}, err
		}
	}
	if _, err := js.CreateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable: "audit", AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		return time.Time{}, err
	}

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "config", Description: "App settings", History: 5})
	if err != nil {
		return time.Time{}, err
	}
	for _, kvp := range [][2]string{{"app.color", "red"}, {"app.color", "blue"}, {"app.size", "10"}, {"app.color", "green"}, {"feature.x", "on"}} {
		if _, err := kv.Put(ctx, kvp[0], []byte(kvp[1])); err != nil {
			return time.Time{}, err
		}
	}
	if err := kv.Delete(ctx, "feature.x"); err != nil {
		return time.Time{}, err
	}

	obs, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "files", Description: "Fixture files"})
	if err != nil {
		return time.Time{}, err
	}
	if _, err := obs.Put(ctx, jetstream.ObjectMeta{Name: "hello.txt", Description: "A greeting",
		Headers: natsio.Header{"Content-Type": []string{"text/plain"}}, Metadata: map[string]string{"owner": "fixture"}},
		bytes.NewReader([]byte("hello world"))); err != nil {
		return time.Time{}, err
	}
	big := make([]byte, 300000)
	for i := range big {
		big[i] = byte(i % 251)
	}
	if _, err := obs.PutBytes(ctx, "data.bin", big); err != nil {
		return time.Time{}, err
	}
	if _, err := obs.PutString(ctx, "old.txt", "stale"); err != nil {
		return time.Time{}, err
	}
	if err := obs.Delete(ctx, "old.txt"); err != nil {
		return time.Time{}, err
	}
	// A plain subscription so connections() and subscriptions() have something to show.
	if _, err := nc.SubscribeSync("orders.>"); err != nil {
		return time.Time{}, err
	}
	if err := nc.Flush(); err != nil {
		return time.Time{}, err
	}
	return boundary, nil
}

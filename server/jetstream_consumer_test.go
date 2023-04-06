package server

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamConsumerMultipleFiltersInitialStateAccounting(t *testing.T) {
	var err error
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three.>", "four"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)
	sendStreamMsg(t, nc, "three.3", "4")
	sendStreamMsg(t, nc, "three.3", "4")
	sendStreamMsg(t, nc, "one", "1")
	sendStreamMsg(t, nc, "two", "2")
	sendStreamMsg(t, nc, "one", "3")
	sendStreamMsg(t, nc, "one", "3")

	deliver := "deliver"
	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "durable",
		DeliverSubject: deliver,
		FilterSubjects: []string{"one", "two"},
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	sub, err := nc.SubscribeSync(deliver)
	require_NoError(t, err)

	for i := 0; i < 4; i++ {
		_, err = sub.NextMsg(time.Second * 1)
		require_NoError(t, err)
	}
}

func TestJetStreamConsumerMultipleFiltersAccounting(t *testing.T) {
	var err error
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, _ := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three.>"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	deliver := "deliver"
	_, err = mset.addConsumer(&ConsumerConfig{
		Durable:        "durable",
		DeliverSubject: deliver,
		FilterSubjects: []string{"one", "two", "three.>"},
		AckPolicy:      AckExplicit,
	})
	require_NoError(t, err)

	sub, err := nc.SubscribeSync(deliver)
	require_NoError(t, err)

	_, err = sub.NextMsg(time.Second * 1)
	require_True(t, err != nil)

	sendStreamMsg(t, nc, "one", "1")
	_, err = sub.NextMsg(time.Second * 1)

	nc.Publish("two", []byte("2"))
	nc.Publish("three", []byte("3"))
	nc.Publish("two", []byte("4"))
	nc.Flush()

	_, err = sub.NextMsg(time.Second * 1)
}

func TestJetStreamConsumerMultipleFiltersMultipleConsumers(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	// Setup few subjects with varying messages count.
	subjects := []struct {
		subject  string
		messages int
		wc       bool
	}{
		{subject: "one", messages: 50},
		{subject: "two", messages: 75},
		{subject: "three", messages: 250},
		{subject: "four", messages: 10},
		{subject: "five.>", messages: 300, wc: true},
	}

	totalMsgs := 0
	for _, subject := range subjects {
		totalMsgs += subject.messages
	}

	// Setup consumers, filtering some of the messages from the stream.
	consumers := []*struct {
		name         string
		subjects     []string
		expectedMsgs int
		delivered    atomic.Int32
	}{
		{name: "C1", subjects: []string{"one", "two"}, expectedMsgs: 125},
		// {name: "C2", subjects: []string{"two", "three"}, expectedMsgs: 1000},
		// {name: "C3", subjects: []string{"one", "three"}, expectedMsgs: 750},
		// {name: "C4", subjects: []string{"one", "five.>"}, expectedMsgs: 800},
	}

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three", "four", "five.>"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

	actualMsgs := []Message{}
	for c, consumer := range consumers {
		_, err := mset.addConsumer(&ConsumerConfig{
			Durable:        consumer.name,
			FilterSubjects: consumer.subjects,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverAll,
			AckWait:        time.Second * 30,
			DeliverSubject: nc.NewInbox(),
		})
		require_NoError(t, err)
		go func(c int, name string) {
			_, err = js.Subscribe("", func(m *nats.Msg) {
				require_NoError(t, m.Ack())
				require_NoError(t, err)
				info, _ := m.Metadata()
				actualMsgs = append(actualMsgs, Message{m.Subject, info.Sequence.Stream})
				consumers[c].delivered.Add(1)

			}, nats.Bind("TEST", name))
			require_NoError(t, err)
		}(c, consumer.name)
	}

	// Publish with random intervals, while consumers are active.
	rand.Seed(time.Now().UnixNano())
	var wg sync.WaitGroup
	for _, subject := range subjects {
		wg.Add(subject.messages)
		go func(subject string, messages int, wc bool) {
			nc, js := jsClientConnect(t, s)
			defer nc.Close()
			time.Sleep(time.Duration(rand.Int63n(1000)+1) * time.Millisecond)
			// fmt.Printf("publishing subject %v with %v messages\n", subject, messages)
			for i := 0; i < messages; i++ {
				time.Sleep(time.Duration(rand.Int63n(1000)+1) * time.Microsecond)
				// If subject has wildcard, add random last subject token.
				pubSubject := subject
				if wc {
					pubSubject = fmt.Sprintf("%v.%v", subject, rand.Int63n(10))
				}
				_, err := js.PublishAsync(pubSubject, []byte("data"))
				require_NoError(t, err)
				wg.Done()
			}
			// fmt.Printf("DONE PUBBLISHING FOR %v\n", subject)
		}(subject.subject, subject.messages, subject.wc)
	}
	wg.Wait()
	time.Sleep(time.Second * 3)

	msgs := []Message{}
	js.Subscribe("", func(msg *nats.Msg) {
		info, err := msg.Metadata()
		require_NoError(t, err)
		msgs = append(msgs, Message{msg.Subject, info.Sequence.Stream})
	}, nats.BindStream("TEST"))

	checkFor(t, time.Second*15, time.Second*1, func() error {
		for _, consumer := range consumers {
			info, err := js.ConsumerInfo("TEST", consumer.name)
			require_NoError(t, err)
			if info.Delivered.Consumer != uint64(consumer.expectedMsgs) {
				fmt.Printf("ACTUAL %+v\n ALL: %+v\n", actualMsgs, msgs)
				fmt.Printf("LEN ACTUAL %v LEN ALL %v\n", len(actualMsgs), len(msgs))
				fmt.Printf("TOTAL: %v\n", totalMsgs)
				findMissing(actualMsgs, msgs)
				totalOneTwo(msgs)

				return fmt.Errorf("%v:expected consumer delivered seq %v, got %v. actually delivered: %v", consumer.name, consumer.expectedMsgs, info.Delivered.Consumer, consumer.delivered.Load())
			}
			if info.AckFloor.Consumer != uint64(consumer.expectedMsgs) {
				return fmt.Errorf("%v: expected consumer ack floor %v, got %v", consumer.name, totalMsgs, info.AckFloor.Consumer)
			}
			if consumer.delivered.Load() != int32(consumer.expectedMsgs) {

				return fmt.Errorf("%v: expected %v, got %v", consumer.name, consumer.expectedMsgs, consumer.delivered)
			}
		}
		// streamInfo, err := js.StreamInfo("TEST")
		// require_NoError(t, err)
		// fmt.Printf("STREAM INFO %+v\n", streamInfo)
		return nil
	})

}

func findMissing(actual, all []Message) {
	act := make(map[uint64]struct{})
	for _, msg := range actual {
		act[msg.sequence] = struct{}{}
	}
	for _, msg := range all {
		if msg.subject == "one" || msg.subject == "two" {

			if _, ok := act[msg.sequence]; !ok {
				fmt.Printf("MISSING MESSAGE!!!: %+v\n", msg)
			}
		}
	}
}

func totalOneTwo(msgs []Message) {
	total := 0
	for _, msg := range msgs {
		if msg.subject == "one" || msg.subject == "two" {
			total++
		}
	}
	fmt.Printf("TOTAL ONE TWO in ALL: %+v\n", total)
}

type Message struct {
	subject  string
	sequence uint64
}

package server

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamConsumerPushMultipleFiltersInterest(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	subjects := []struct {
		subject  string
		messages int
	}{
		{subject: "one", messages: 500},
		{subject: "two", messages: 500},
		{subject: "three", messages: 500},
		{subject: "four", messages: 500},
	}

	totalMsgs := 0
	for _, subject := range subjects {
		totalMsgs += subject.messages
	}

	consumers := []struct {
		name         string
		subjects     []string
		expectedMsgs int
		delivered    atomic.Int32
	}{
		{name: "C1", subjects: []string{"one", "two"}, expectedMsgs: 1000},
		// {name: "C2", subjects: []string{"one", "three"}, expectedMsgs: 1000},
		// {name: "C3", subjects: []string{"two", "three"}, expectedMsgs: 1000},
	}

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three", "four"},
		MaxAge:    time.Second * 90,
	})
	require_NoError(t, err)

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

				consumers[c].delivered.Add(1)

			}, nats.Bind("TEST", name))
			require_NoError(t, err)
		}(c, consumer.name)
	}

	for _, subject := range subjects {
		go func(subject string, messages int) {
			for i := 0; i < messages; i++ {
				_, err := js.PublishAsync(subject, []byte("data"))
				require_NoError(t, err)
			}
		}(subject.subject, subject.messages)
	}

	checkFor(t, time.Second*5, time.Second*1, func() error {
		// for _, consumer := range consumers {
		consumer := consumers[0]
		fmt.Println("in info for ", consumer.name)
		info, err := js.ConsumerInfo("TEST", consumer.name)
		require_NoError(t, err)
		// streamInfo, err := js.StreamInfo("TEST")
		// require_NoError(t, err)
		// fmt.Printf("INFO: %+v\n", streamInfo.State)
		// fmt.Printf("INFO: %+v\n", streamInfo.Config)
		// fmt.Printf("CONSUMER INFO: %+v\n", info)

		if info.Delivered.Consumer != uint64(consumer.expectedMsgs) {
			return fmt.Errorf("%v:expected %v delivered, got %v", consumer.name, consumer.expectedMsgs, info.Delivered.Consumer)
		}
		if info.AckFloor.Consumer != uint64(consumer.expectedMsgs) {
			return fmt.Errorf("%v: expected consumer ack floor %v, got %v", consumer.name, totalMsgs, info.AckFloor.Consumer)
		}
		if consumer.delivered.Load() != int32(consumer.expectedMsgs) {

			return fmt.Errorf("%v: expected %v, got %v", consumer.name, consumer.expectedMsgs, consumer.delivered)
		}
		// }
		// streamInfo, err := js.StreamInfo("TEST")
		// require_NoError(t, err)
		// fmt.Printf("INFO: %+v\n", streamInfo.State)
		// fmt.Printf("INFO: %+v\n", streamInfo.Config)
		// if info.State.Msgs != 0 {
		// 	return fmt.Errorf("expected 0 messages in stream, got %v", info.State.Msgs)
		// }
		// if info.State.NumDeleted != 123 {
		// 	return fmt.Errorf("expected 123 deleted messages, got %v", info.State.NumDeleted)
		// }
		return nil
	})

}
func TestJetStreamConsumerPullMultipleFiltersInterest(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	subjects := []struct {
		subject  string
		messages int
	}{
		{subject: "one", messages: 1000},
		{subject: "two", messages: 500},
		{subject: "three", messages: 200},
		{subject: "four", messages: 123},
	}

	totalMsgs := 0
	for _, subject := range subjects {
		totalMsgs += subject.messages
	}

	consumers := []struct {
		name         string
		subjects     []string
		expectedMsgs int
		delivered    int
	}{
		{name: "C1", subjects: []string{"one", "two"}, expectedMsgs: 1500},
		{name: "C2", subjects: []string{"one", "three"}, expectedMsgs: 1200},
		{name: "C3", subjects: []string{"two", "three"}, expectedMsgs: 700},
	}

	mset, err := acc.addStream(&StreamConfig{
		Name: "TEST",
		// Retention: InterestPolicy,
		Retention: LimitsPolicy,
		Subjects:  []string{"one", "two", "three", "four"},
		MaxAge:    time.Second * 90,
		// MaxMsgs:   1000000,
	})
	require_NoError(t, err)

	for c, consumer := range consumers {
		_, err := mset.addConsumer(&ConsumerConfig{
			Durable:        consumer.name,
			FilterSubjects: consumer.subjects,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverAll,
			AckWait:        time.Second * 30,
		})
		require_NoError(t, err)
		sub, err := js.PullSubscribe("", consumer.name, nats.Bind("TEST", consumer.name))
		require_NoError(t, err)

		go func(toReceive int, c int) {
			i := 0
			for i < toReceive {
				msgs, _ := sub.Fetch(100)
				i += len(msgs)
				// require_NoError(t, err)
				for _, msg := range msgs {
					require_NoError(t, msg.Ack())
					consumers[c].delivered++
				}
			}
		}(consumer.expectedMsgs, c)
	}

	for _, subject := range subjects {
		go func(subject string, messages int) {
			for i := 0; i < messages; i++ {
				_, err := js.PublishAsync(subject, []byte("data"))
				require_NoError(t, err)
			}
		}(subject.subject, subject.messages)
	}

	checkFor(t, time.Second*10, time.Millisecond*200, func() error {
		for _, consumer := range consumers {
			fmt.Println("in info for ", consumer.name)
			info, err := js.ConsumerInfo("TEST", consumer.name)
			require_NoError(t, err)
			streamInfo, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			fmt.Printf("INFO: %+v\n", streamInfo.State)
			fmt.Printf("INFO: %+v\n", streamInfo.Config)

			if info.Delivered.Consumer != uint64(consumer.expectedMsgs) {
				fmt.Printf("CONSUMER INFO: %+v\n", info)
				return fmt.Errorf("%v:expected %v delivered, got %v", consumer.name, consumer.expectedMsgs, info.Delivered.Consumer)
			}
			if info.AckFloor.Consumer != uint64(consumer.expectedMsgs) {
				return fmt.Errorf("%v: expected consumer ack floor %v, got %v", consumer.name, totalMsgs, info.AckFloor.Consumer)
			}
			if consumer.delivered != consumer.expectedMsgs {

				return fmt.Errorf("%v: expected %v, got %v", consumer.name, consumer.expectedMsgs, consumer.delivered)
			}
		}
		streamInfo, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		fmt.Printf("INFO: %+v\n", streamInfo.State)
		fmt.Printf("INFO: %+v\n", streamInfo.Config)
		// if info.State.Msgs != 0 {
		// 	return fmt.Errorf("expected 0 messages in stream, got %v", info.State.Msgs)
		// }
		// if info.State.NumDeleted != 123 {
		// 	return fmt.Errorf("expected 123 deleted messages, got %v", info.State.NumDeleted)
		// }
		return nil
	})

}

func TestJetStreamConsumerPullSingleFiltersInterest(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	acc := s.GlobalAccount()

	subjects := []struct {
		subject  string
		messages int
	}{
		{subject: "one", messages: 1000},
		{subject: "two", messages: 500},
		{subject: "three", messages: 200},
		{subject: "four", messages: 123},
	}

	totalMsgs := 0
	for _, subject := range subjects {
		totalMsgs += subject.messages
	}

	consumers := []struct {
		name         string
		subjects     []string
		expectedMsgs int
		delivered    int
	}{
		{name: "C1", subjects: []string{"one"}, expectedMsgs: 1000},
		{name: "C2", subjects: []string{"three"}, expectedMsgs: 200},
		{name: "C3", subjects: []string{"two"}, expectedMsgs: 500},
	}

	mset, err := acc.addStream(&StreamConfig{
		Name:      "TEST",
		Retention: InterestPolicy,
		Subjects:  []string{"one", "two", "three", "four"},
		// MaxAge:    time.Second * 90,
		// MaxMsgs:   1000000,
	})
	require_NoError(t, err)

	for c, consumer := range consumers {
		_, err := mset.addConsumer(&ConsumerConfig{
			Durable:        consumer.name,
			FilterSubjects: consumer.subjects,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverAll,
			AckWait:        time.Second * 30,
		})
		require_NoError(t, err)
		sub, err := js.PullSubscribe("", consumer.name, nats.Bind("TEST", consumer.name))
		require_NoError(t, err)

		go func(toReceive int, c int) {
			i := 0
			for i < toReceive {
				msgs, _ := sub.Fetch(100)
				i += len(msgs)
				// require_NoError(t, err)
				for _, msg := range msgs {
					require_NoError(t, msg.Ack())
					consumers[c].delivered++
				}
			}
		}(consumer.expectedMsgs, c)
	}

	for _, subject := range subjects {
		go func(subject string, messages int) {
			for i := 0; i < messages; i++ {
				_, err := js.PublishAsync(subject, []byte("data"))
				require_NoError(t, err)
			}
		}(subject.subject, subject.messages)
	}

	checkFor(t, time.Second*10, time.Millisecond*200, func() error {
		for _, consumer := range consumers {
			fmt.Println("in info for ", consumer.name)
			info, err := js.ConsumerInfo("TEST", consumer.name)
			require_NoError(t, err)

			streamInfo, err := js.StreamInfo("TEST")
			require_NoError(t, err)
			fmt.Printf("INFO: %+v\n", streamInfo.State)
			fmt.Printf("INFO: %+v\n", streamInfo.Config)
			if info.Delivered.Consumer != uint64(consumer.expectedMsgs) {
				fmt.Printf("CONSUMER INFO: %+v\n", info)
				return fmt.Errorf("%v:expected %v delivered, got %v", consumer.name, consumer.expectedMsgs, info.Delivered.Consumer)
			}
			if info.AckFloor.Consumer != uint64(consumer.expectedMsgs) {
				return fmt.Errorf("%v: expected consumer ack floor %v, got %v", consumer.name, totalMsgs, info.AckFloor.Consumer)
			}
			if consumer.delivered != consumer.expectedMsgs {
				fmt.Printf("%v:INFO: %+v", consumer.name, info)
				return fmt.Errorf("%v: expected %v, got %v", consumer.name, consumer.expectedMsgs, consumer.delivered)
			}
		}
		info, err := js.StreamInfo("TEST")
		require_NoError(t, err)
		fmt.Printf("INFO: %+v\n", info.State)
		fmt.Printf("INFO: %+v\n", info.Config)
		// if info.State.Msgs != 0 {
		// 	return fmt.Errorf("expected 0 messages in stream, got %v", info.State.Msgs)
		// }
		// if info.State.NumDeleted != 123 {
		// 	return fmt.Errorf("expected 123 deleted messages, got %v", info.State.NumDeleted)
		// }
		return nil
	})

}

package msg

import (
	"testing"
	"time"
)

func TestSending(t *testing.T) {

	Handle("ping", func(m Message) Message {
		return Ack
	})

	go func() {
		time.Sleep(2 * time.Second)
		client, err := NewClient("localhost:3000")
		if err != nil {
			t.Fatal(err)
		}

		res := client.Send(Ping)

		if res.action != "ack" {
			t.Fatal(res.action)
		}
	}()

	go Serve("0.0.0.0:3000")

	time.Sleep(5 * time.Second)
}

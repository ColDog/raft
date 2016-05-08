package main

import (
	"github.com/coldog/raft/msg"
	"time"
	"log"
)

// LEADER = 2
// FOLLOWER = 1
// CANDIDATE = 0

func main() {
	msg.Handle("ping", func(m msg.Message) msg.Message {
		return msg.Ack
	})

	go func() {
		time.Sleep(2 * time.Second)
		client, err := msg.NewClient("localhost:3000")
		if err != nil {
			log.Fatal(err)
		}

		res := client.Send(msg.Ping)
		log.Printf("returned: %v", res)
	}()

	go msg.Serve("0.0.0.0:3000")

	time.Sleep(5 * time.Second)
}

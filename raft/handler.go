package raft

import (
	"time"
	"github.com/coldog/raft/msg"
	"log"
)

type RequestHandler struct {
	Msg 	msg.Message
	Resp 	chan msg.Message
}

func (req *RequestHandler) Respond(m msg.Message)  {
	select {
	case req.Resp <- m:
	default:
	}
}

func (raft *Raft) handle(m msg.Message) msg.Message {
	handler := &RequestHandler{m, make(chan msg.Message)}

	select {
	case raft.appChan <- handler:
		select {
		case res := <- handler.Resp:
			return res
		}
	case <-time.After(50 * time.Millisecond):
		select {
		case raft.quit <- true:
			log.Println("no channel available for append entries quitting")
			return msg.ServerError
		default:
			log.Println("no channel available for append entries failed to quit")
			return msg.ServerError
		}
	}
}

func (raft *Raft) RegisterHandlers() {
	msg.Handle(raft.name("abort"), raft.handle)
	msg.Handle(raft.name("commit"), raft.handle)
	msg.Handle(raft.name("append"), raft.handle)

	msg.Handle(raft.name("ping"), func(m msg.Message) msg.Message {
		return raft.BaseMessage(raft.name("ping"))
	})

	msg.Handle(raft.name("vote"), func(m msg.Message) msg.Message {
		c := make(chan msg.Message)
		raft.vteChan <- &RequestHandler{m, c}

		select {
		case res := <- c:
			return res
		case <-time.After(50 * time.Millisecond):
			return msg.ServerError
		}
	})
}

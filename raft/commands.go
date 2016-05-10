package raft

import (
	"github.com/coldog/raft/msg"
	"github.com/coldog/raft/store"
	"log"
)


type AppendResponse struct {
	Id 	int64
	Err 	error
}

func (raft *Raft) AckMessage() msg.Message {
	return raft.BaseMessage("ack")
}

func (raft *Raft) ErrMessage(err string) msg.Message {
	m := raft.BaseMessage("ack")
	m.MarkFailed()
	m.Params["error"] = err
	return m
}

func (raft *Raft) BaseMessage(action string) msg.Message {
	m := msg.NewMessage(action)
	m.Params["term"] = raft.Term
	m.Params["from"] = raft.Cluster.Self
	m.Params["leader"] = raft.Leader
	m.Params["last_id"] = store.LastKey
	m.Params["size"] = store.Size
	return m
}

func (raft *Raft) VoteMessage(id string) msg.Message {
	m := raft.BaseMessage("raft.vote")
	m.Params["vote"] = id
	return m
}


func (raft *Raft) RequestVoteMessage() msg.Message {
	m := raft.BaseMessage("raft.vote")
	m.Params["vote"] = raft.Cluster.Self
	return m
}


func (raft *Raft) AppendEntriesMessage(entries []store.Entry) msg.Message {
	m := raft.BaseMessage("raft.append")
	es := make([][]byte, 0)
	ids := make([]int64, 0)
	for _, entry := range entries {
		ids = append(ids, entry.Id)
		es = append(es, entry.Entry)
	}

	m.Params["ids"] = ids
	m.Params["entries"] = es

	log.Printf("append entries message %v", m.Params)
	return m
}

func (raft *Raft) CommitMessage(entries []store.Entry) msg.Message {
	m := raft.BaseMessage("raft.commit")

	ids := make([]int64, 0)
	for _, entry := range entries {
		ids = append(ids, entry.Id)
	}

	m.Params["ids"] = ids
	return m
}

func (raft *Raft) AbortMessage(entries []store.Entry) msg.Message {
	m := raft.BaseMessage("raft.abort")

	ids := make([]int64, 0)
	for _, entry := range entries {
		ids = append(ids, entry.Id)
	}

	m.Params["ids"] = ids
	return m
}

func (raft *Raft) PingMessage() msg.Message {
	return raft.BaseMessage("raft.ping")
}

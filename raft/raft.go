package raft

import (
	"github.com/coldog/raft/msg"
	"github.com/coldog/raft/store"

	"time"
	"log"
	"sync"
	"errors"
)

// LEADER = 2
// FOLLOWER = 1
// CANDIDATE = 0

const (
	LEADER_WAIT_TIME time.Duration = 5000 * time.Millisecond
	FOLLOWER_TIMEOUT time.Duration = 10000 * time.Millisecond
	CANDIDATE_TIMEOUT time.Duration = 5000 * time.Millisecond
)

func NewRaft(id string, address string, server string, storeType string) *Raft {
	r := &Raft{
		State: 0,
		Term: 0,
		Leader: "",

		quit: make(chan bool),
		reqChan: make(chan store.Entry),
		resChan: make(chan AppendResponse),
		appChan: make(chan *RequestHandler),
		vteChan: make(chan *RequestHandler),

		Cluster: &Cluster{
			Nodes: make(map[string] *Node),
			Self: id,
			lock: sync.RWMutex{},
			events: make(chan string),
		},
	}

	if storeType == "mem" {
		r.store = store.NewMemStore(id)
	} else {
		r.store = store.NewBoltStore(id)
	}

	r.Cluster.Nodes[id] = &Node{
		Id: id,
		Url: address,
		Up: true,
		client: &msg.Client{},
	}

	go msg.Serve(server)
	go r.Run()
	go r.Cluster.Publisher()
	r.Cluster.RegisterClusterMessages()
	r.RegisterHandlers()
	return r
}

type Raft struct {
	State 	int

	Cluster *Cluster

	Term 	int64
	Leader	string

	store 	store.Store

	quit 	chan bool

	// a channel where log requests are pushed to the leader.
	reqChan	chan store.Entry
	resChan chan AppendResponse

	// server requests pushed along through the channels
	appChan chan *RequestHandler
	vteChan chan *RequestHandler
}

func (raft *Raft) NewIterator(start []byte) *store.Iterator {
	return raft.store.NewIterator(start)
}

func (raft *Raft) ToLeader() {
	raft.Leader = raft.Cluster.Self
	raft.Term += 1
	raft.State = 2
}

func (raft *Raft) ToCandidate() {
	raft.State = 0
}

func (raft *Raft) ToFollower(id string, term int64) {
	raft.Leader = id
	raft.Term = term
	raft.State = 1
}

func (raft *Raft) IsLeader() bool {
	return raft.State == 2
}

func (raft *Raft) IsCandidate() bool {
	return raft.State == 0
}

func (raft *Raft) IsFollower() bool {
	return raft.State == 1
}

func (raft *Raft) RunAsCandidate() {
	votes := make(map[string] bool)

	for {
		hasVoted := false
		select {
		case <- raft.quit:
			return

		case <- raft.reqChan:
			log.Printf("tried to initiate append on non-leader")

		case <- raft.Cluster.events:
			log.Printf("not leader, will not bring up node")

		case req := <- raft.appChan:
			raft.handleRaftAppend(req)
			raft.ToFollower(req.Msg.Params["from"].(string), req.Msg.Params["term"].(int64))
			req.Respond(raft.AckMessage())
			return

		case req := <- raft.vteChan:
			if asserted, ok := req.Msg.Params["vote"].(string); ok {
				if asserted == raft.Cluster.Self {
					votes[asserted] = true
				} else {
					if !hasVoted {
						hasVoted = true
						raft.Cluster.Publish(raft.VoteMessage(asserted))
					}
				}
			}
			req.Respond(raft.AckMessage())


			if len(votes) >= len(raft.Cluster.Nodes) / 2 {
				log.Printf("moving to leader %v", votes)
				raft.ToLeader()
				return
			}

		// if we don't receive any votes, send out a request vote message
		case <- time.After(CANDIDATE_TIMEOUT):
			if !hasVoted {
				log.Println("requesting vote")
				raft.Cluster.Publish(raft.RequestVoteMessage())
			}
		}
	}
}


func (raft *Raft) RunAsFollower() {
	for {
		select {
		case <- raft.quit:
			return

		case <- raft.reqChan:
			log.Printf("tried to initiate append on non-leader")

		case <- raft.Cluster.events:
			log.Printf("not leader, will not bring up node")

		case req := <- raft.vteChan:
			req.Respond(raft.AckMessage())
			raft.ToCandidate()
			return

		case req := <- raft.appChan:
			raft.handleRaftAppend(req)
			req.Respond(raft.AckMessage())

		// if we don't hear from the leader for a while restart.
		case <-time.After(FOLLOWER_TIMEOUT):
			log.Println("follower timed out")
			raft.ToCandidate()
			return

		}
	}
}


// The leader accepts messages from clients
func (raft *Raft) RunAsLeader() {
	entries := make([]store.Entry, 0)

	// Accrue messages for 150 milliseconds before sending the append entries.
	for {
		select {
		case <- raft.quit:
			return

		case entry := <- raft.reqChan:
			entries = append(entries, entry)

		// receives messages down the
		case nId := <- raft.Cluster.events:
			go raft.BringUpNode(nId)

		case req := <- raft.appChan:
			if req.Msg.Params["term"].(int64) > raft.Term {
				raft.ToFollower(req.Msg.Params["from"].(string), req.Msg.Params["term"].(int64))
				return
			} else {
				raft.ErrMessage("GREATER_TERM_EXCEPTION")
			}

		case <- time.After(LEADER_WAIT_TIME):
			raft.handlePushAppend(entries)
			entries = make([]store.Entry, 0)
		}
	}
}

func (raft *Raft) AddEntry(entry []byte) error {
	if raft.State != 2 {
		return errors.New("NOT_LEADER")
	}

	log.Println("pushing entry")
	e := store.NewEntry(entry)
	raft.reqChan <- e

	for {
		select {
		case res := <- raft.resChan:
			if res.Id == e.KeyAsInt() {
				return res.Err
			}
		}
	}
}

func (raft *Raft) handlePushAppend(entries []store.Entry) {
	log.Printf("pushing append %v", entries)


	// append locally
	for _, entry := range entries {
		raft.store.AppendWithStatus(entry.Key, entry.Entry, entry.Status)
	}

	err := raft.Cluster.BroadcastQuorum(raft.AppendEntriesMessage(entries))
	if err == nil {
		// commit the entries
		for _, entry := range entries {
			raft.store.Commit(entry.Key)
		}

		// send the commit message
		if len(entries) > 0 {
			raft.Cluster.Broadcast(raft.CommitMessage(entries))
		}
	} else {
		if err.Error() == "GREATER_TERM_EXCEPTION" {
			raft.ToCandidate()
			return
		}

		// abort the entries
		for _, entry := range entries {
			raft.store.Abort(entry.Key)
		}

		// send the abort message
		if len(entries) > 0 {
			raft.Cluster.Broadcast(raft.AbortMessage(entries))
		}
	}

	// notify all
	for _, entry := range entries {
		select {
		case raft.resChan <- AppendResponse{entry.KeyAsInt(), err}:
		default:
		}
	}
}


func (raft *Raft) handleRaftAppend(req *RequestHandler) {
	log.Printf("handling append %v", req.Msg)

	t := req.Msg.Params["term"].(int64)
	if raft.Term > t {
		log.Printf("greater term exception %v %v", raft.Term, t)
		raft.ErrMessage("GREATER_TERM_EXCEPTION")
		return
	} else if raft.Term < t {
		raft.Term = t
	}

	raft.Leader = req.Msg.Params["from"].(string)

	if req.Msg.Params["ids"] == nil {
		req.Respond(msg.Ack)
		return
	}


	if req.Msg.Name() == "raft.append" {
		entries := req.Msg.Params["entries"].([][]byte)
		statuses := req.Msg.Params["statuses"].([]int)

		for idx, id := range req.Msg.Params["ids"].([][]byte) {
			entry := entries[idx]
			status := statuses[idx]
			raft.store.AppendWithStatus(id, entry, status)
		}
	}

	if req.Msg.Name() == "raft.commit" {
		log.Println("inside commit")
		for _, id := range req.Msg.Params["ids"].([][]byte) {
			raft.store.Commit(id)
		}
	}

	if req.Msg.Name() == "raft.abort" {
		log.Printf("aborting %v", req.Msg.Params["ids"])
		for _, id := range req.Msg.Params["ids"].([][]byte) {
			raft.store.Abort(id)
		}
	}

	req.Respond(msg.Ack)
}

func (raft *Raft) BringUpNode(id string) {
	log.Printf("bringing up node %v", id)
	nodeInfo := raft.Cluster.Send(id, raft.PingMessage()).Params
	last := nodeInfo["last_id"].([]byte)

	iterator := raft.store.NewIterator(last)

	for {
		entries := iterator.NextCount(5)
		if len(entries) == 0 {
			break
		}

		time.Sleep(150 * time.Millisecond)
		raft.Cluster.Send(id, raft.AppendEntriesMessage(entries))
	}

	raft.Cluster.MarkUp(id)
}


func (raft *Raft) Run() {
	for {
		log.Printf("running %v", raft.State)

		switch raft.State {
		case 0:
			raft.RunAsCandidate()
		case 1:
			raft.RunAsFollower()
		case 2:
			raft.RunAsLeader()
		}
	}
}


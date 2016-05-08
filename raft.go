package main
//
//import (
//	"time"
//)
//
//// LEADER = 2
//// FOLLOWER = 1
//// CANDIDATE = 0
//
//type Raft struct {
//	nodes 	map[string] *Node
//	self 	string
//	locAddr string
//	term 	int64
//
//	// a channel where log requests are pushed to the leader.
//	reqChan	chan []byte
//}
//
//func (raft *Raft) Self() *Node {
//	return raft.nodes[raft.self]
//}
//
//
//
//// The leader accepts messages from clients
//func (raft *Raft) RunAsLeader() {
//	buff := make([][]byte, 0)
//
//	// Accrue messages for 150 milliseconds before sending the append entries.
//	for {
//		select {
//		case msg := <- raft.reqChan:
//			buff = append(buff, msg)
//
//		case <-time.After(150 * time.Millisecond):
//
//			for _, node := range raft.nodes {
//				node.Client.Send()
//			}
//
//		}
//	}
//}
//
//func (raft *Raft) AppendEntriesMessage(entries []byte) {
//
//}

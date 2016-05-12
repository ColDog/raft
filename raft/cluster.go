package raft

import (
	"github.com/coldog/raft/msg"

	"log"
	"errors"
	"sync"
	"time"
	"fmt"
)

type Node struct {
	Id 	string
	Url 	string
	Up 	bool
	Info 	map[string] interface{}
	client 	*msg.Client
}

func (node *Node) Status() string {
	if node.Up {
		return "AVAILABLE"
	} else {
		return "UNAVAILABLE"
	}
}

type Cluster struct {
	Id 	string
	Nodes 	map[string] *Node
	Self 	string
	lock 	sync.RWMutex
	events 	chan string
}

func (cluster *Cluster) name(key string) string {
	return fmt.Sprintf("raft.%s.%s", cluster.Id, key)
}

func (cluster *Cluster) Has(id string) bool  {
	cluster.lock.RLock()
	_, ok := cluster.Nodes[id]
	cluster.lock.RUnlock()
	return ok
}

// adds a new node into the cluster.
func (cluster *Cluster) Add(id, target string) error {
	log.Printf("attempting to add node: %v", target)
	cluster.lock.RLock()
	_, ok := cluster.Nodes[id]
	cluster.lock.RUnlock()

	if ok {
		return errors.New("Already Exists")
	}

	c, err := msg.NewClient(target)
	if err != nil {
		return err
	}

	cluster.lock.Lock()
	defer cluster.lock.Unlock()

	cluster.Nodes[id] = &Node{
		Id: id,
		Url: target,
		Up: false,
		Info: make(map[string] interface{}),
		client: c,
	}
	cluster.events <- id

	log.Printf("added node: %v", target)
	return nil
}

func (cluster *Cluster) Info(id string) map[string] interface{} {
	cluster.lock.RLock()
	defer cluster.lock.RUnlock()


	if node, ok := cluster.Nodes[id]; ok {
		return node.Info
	} else {
		return nil
	}

}

func (cluster *Cluster) MarkUp(id string) {
	cluster.mark(id, true)
}

func (cluster *Cluster) MarkDown(id string) {
	cluster.mark(id, false)
}

func (cluster *Cluster) mark(id string, value bool) {
	cluster.lock.RLock()
	node, ok := cluster.Nodes[id]
	cluster.lock.RUnlock()

	if ok && !node.Up {
		cluster.lock.Lock()
		defer cluster.lock.Unlock()

		cluster.Nodes[id].Up = value
		cluster.events <- id
	}
}

// broadcasts a message to the cluster, errors if quorum errors
func (cluster *Cluster) BroadcastQuorum(m msg.Message) error {
	cluster.lock.RLock()
	defer cluster.lock.RUnlock()

	errs := make([]error, 0)

	for _, node := range cluster.Nodes {
		if node.Id != cluster.Self {
			res := node.client.Send(m)
			if res.Failed() {
				log.Printf("broadcast failed: %s, %v", res.Name(), res.Params)
				errs = append(errs, errors.New(res.Name()))
			}
		}

		if len(errs) >= len(cluster.Nodes) / 2 {
			return errors.New("QUORUM_FAILED")
		}
	}

	return nil
}

// broadcasts a message to the cluster, errors if any error
func (cluster *Cluster) Broadcast(m msg.Message) error {
	cluster.lock.RLock()
	defer cluster.lock.RUnlock()

	for _, node := range cluster.Nodes {
		if node.Id != cluster.Self {
			res := node.client.Send(m)
			if res.Failed() {
				log.Printf("broadcast failed: %s, %v", res.Name(), res.Params)
				return errors.New(res.Name())
			}
		}
	}

	return nil
}

// broadcasts a message to the cluster, will not error
func (cluster *Cluster) Publish(m msg.Message) {
	go func() {
		cluster.lock.RLock()
		defer cluster.lock.RUnlock()

		for _, node := range cluster.Nodes {
			if node.Id != cluster.Self {
				node.client.Send(m)
			}
		}
	}()
}

// sends a message to a specific node, errors if any error
func (cluster *Cluster) Send(id string, m msg.Message) msg.Message {
	cluster.lock.RLock()
	defer cluster.lock.RUnlock()

	if node, ok := cluster.Nodes[id]; ok {
		res := node.client.Send(m)
		return res
	}

	return msg.ServerError
}

func (cluster *Cluster) Publisher() {
	go func() {
		for {
			cluster.Publish(cluster.ClusterMessage())
			time.Sleep(time.Duration(5 * len(cluster.Nodes)) * time.Second)
		}
	}()
}

// message about the cluster state and which nodes are connected
func (cluster *Cluster) ClusterMessage() msg.Message {
	m := msg.NewMessage(cluster.name("cluster"))

	for _, node := range cluster.Nodes {
		m.Params[node.Id] = []string{node.Url, node.Status()}
	}

	return m
}

// handles cluster specific traffic. this keeps up to date a view of the cluster from all nodes.
func (cluster *Cluster) RegisterClusterMessages()  {
	msg.Handle(cluster.name("cluster"), func(m msg.Message) msg.Message {
		for id, val := range m.Params {
			props := val.([]string)

			if !cluster.Has(id) {
				cluster.Add(id, props[0])
			}

			if props[1] == "UNAVAILABLE" {
				cluster.MarkDown(id)
			} else {
				cluster.MarkUp(id)
			}
		}

		return msg.Ack
	})
}

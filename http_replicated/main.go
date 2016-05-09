package http_replicated

import (
	"github.com/coldog/raft/raft"
	"github.com/julienschmidt/httprouter"

	"net/http"
	"log"
	"encoding/json"
	"flag"
	"fmt"
	"bytes"
	"encoding/binary"
	"github.com/coldog/raft/store"
)

var raftCluster *raft.Raft

func handleAppend(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()
	err := raftCluster.AddEntry([]byte(r.Form.Get("entry")))
	j, _ := json.Marshal(map[string] interface{} {
		"error": err,
	})
	w.Write(j)
}

func HandleView(w http.ResponseWriter, r *http.Request, _ httprouter.Params)  {
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()

	start := toInt64(r.Form.Get("start"))
	results := make(map[string] string)

	for {
		key, val := store.Next(start)
		if key == 0 {
			break
		}

		start = key
		results[fmt.Sprintf("%d", key)] = string(val)
	}

	j, e := json.Marshal(results)
	if e != nil {
		panic(e)
	}
	w.Write(j)
}

func handleAddNode(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()

	err := raftCluster.Cluster.Add(r.Form.Get("node_id"), r.Form.Get("address"))
	j, _ := json.Marshal(map[string] interface{} {
		"error": err,
	})
	w.Write(j)
}

func handleViewCluster(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	j, _ := json.Marshal(raftCluster)
	w.Write(j)
}

func Main() {
	id := flag.String("id", "node1", "This node's id")
	address := flag.String("address", "localhost:3000", "This node's address")
	server := flag.String("server", "0.0.0.0:3000", "This node's server to bind")
	httpServer := flag.String("http", "0.0.0.0:8080", "This node's http server to bind on")
	flag.Parse()

	fmt.Printf("id:      %v\n", *id)
	fmt.Printf("address: %v\n", *address)
	fmt.Printf("server:  %v\n", *server)
	print("\n\n")

	raftCluster = raft.NewRaft(*id, *address, *server)

	router := httprouter.New()
	router.POST("/cluster", handleAddNode)
	router.GET("/cluster", handleViewCluster)
	router.POST("/logs", handleAppend)
	router.GET("/logs", HandleView)

	log.Fatal(http.ListenAndServe(*httpServer, router))
}

func toInt64(data string) int64 {
	var value uint64
	buf := bytes.NewReader([]byte(data))
	binary.Read(buf, binary.LittleEndian, &value)
	return int64(value)
}
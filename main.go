package main

import (
	"github.com/coldog/raft/raft"
	"github.com/julienschmidt/httprouter"

	"net/http"
	"log"
	"encoding/json"
	"flag"
	"fmt"
)

var raftCluster *raft.Raft

func ok(w http.ResponseWriter, msg string)  {
	w.Header().Set("Content-Type", "application/json")
	j, _ := json.MarshalIndent(map[string] interface{} {
		"error": nil,
		"message": msg,
	}, "", "  ")
	w.Write(j)
}

func fail(w http.ResponseWriter, err error)  {
	w.Header().Set("Content-Type", "application/json")
	j, _ := json.MarshalIndent(map[string] interface{} {
		"error": err.Error(),
		"messaage": "Request Failed",
	}, "", "  ")
	w.Write(j)
}

func handleAppend(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	r.ParseForm()
	err := raftCluster.AddEntry([]byte(r.Form.Get("entry")))
	if err != nil {
		fail(w, err)
	} else {
		ok(w, "Appended entry")
	}
}

func HandleView(w http.ResponseWriter, r *http.Request, _ httprouter.Params)  {
	w.Header().Set("Content-Type", "application/json")
	r.ParseForm()

	results := make([]map[string] interface{}, 0)
	it := raftCluster.NewIterator([]byte{0})
	entries := it.NextCount(100)

	for _, entry := range entries {
		current := make(map[string] interface{})
		current["key"] = entry.KeyAsInt()
		current["value"] = string(entry.Entry)
		current["status"] = entry.Status
		results = append(results, current)
	}

	j, e := json.MarshalIndent(results, "", "  ")
	if e != nil {
		log.Printf("error parsing json %v", e)
	} else {
		w.Write(j)
	}
}

func handleAddNode(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	r.ParseForm()

	err := raftCluster.Cluster.Add(r.Form.Get("node_id"), r.Form.Get("address"))
	if err != nil {
		fail(w, err)
	} else {
		ok(w, "Added Node")
	}
}

func handleViewCluster(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	j, _ := json.MarshalIndent(raftCluster, "", "  ")
	w.Write(j)
}

func main() {
	id := flag.String("id", "node1", "This node's id")
	address := flag.String("address", "localhost:3000", "This node's address")
	server := flag.String("server", "0.0.0.0:3000", "This node's server to bind")
	httpServer := flag.String("http", "0.0.0.0:8080", "This node's http server to bind on")
	storeType := flag.String("store", "bolt", "This node's store type")
	flag.Parse()

	fmt.Printf("id:      %v\n", *id)
	fmt.Printf("address: %v\n", *address)
	fmt.Printf("server:  %v\n", *server)
	fmt.Printf("http:    %v\n", *httpServer)
	fmt.Printf("store:   %v\n", *storeType)
	print("\n\n")

	raftCluster = raft.NewRaft(*id, *address, *server, *storeType)
	raft.Initialize()

	router := httprouter.New()
	router.POST("/cluster", handleAddNode)
	router.GET("/cluster", handleViewCluster)
	router.POST("/logs", handleAppend)
	router.GET("/logs", HandleView)

	log.Fatal(http.ListenAndServe(*httpServer, router))
}

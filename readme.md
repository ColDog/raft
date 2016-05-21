# Raft Implementation

This is a raft implementation written in Golang.

### Quickstart

### Development Quickstart

The main.go holds a quickstart example of a http server that replicates the raft log using simple string entries. You can view all the entries in the cluster, add entries and add nodes using http.

To experiment with this there are some bash scripts in the `bin` directory. Run the following commands to get a 3 node development cluster up and running.

```bash
go get ...                # install all needed golang packages
bin/test-node 0           # start up a test node on localhost:3000 with http on localhost:8000
bin/test-node 1           # start up a test node on localhost:3001 with http on localhost:8001
bin/test-node 2           # start up a test node on localhost:3002 with http on localhost:8002
bin/test-client join 0 1  # join the first node to the second
bin/test-client join 0 2  # join the first node to the third
```

Now you have a test cluster from the latest source running on localhost. You can play around with the commands in the test client script.

```bash
# append an entry to a test node (note, you may get back a not leader error)
bin/test-client append 0 test_entry_1
bin/test-client append 0 test_entry_2

# view all the added entries on node 0
bin/test-client all 0
```


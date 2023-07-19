package fdraft

import (
	"fmt"
	"net"
	"net/rpc"
	"sync/atomic"
	// "time"

	"github.com/sasha-s/go-deadlock"
)

type Server struct {
	id        int
	rwMu      deadlock.RWMutex
	listener  net.Listener
	server    *rpc.Server
	peers     map[int]*rpc.Client
	dead      atomic.Bool
	config    Config
	beginChan chan int
	Proxy     *Proxy
}

// Prevent from exporting all the methods of consensus module and KV server
type Proxy struct {
	kvServer *KVServer
}

func MakeServer(id, batching, nodeCount int, wg *deadlock.WaitGroup) *Server {
	server := new(Server)
	server.id = id
	server.beginChan = make(chan int)
	server.dead.Store(false)
	server.peers = make(map[int]*rpc.Client)
	server.config = BuildConfig(nodeCount)
	persister := MakePersister(id)
	server.Proxy = new(Proxy)
	server.Proxy.kvServer = StartKVServer(server.config.Nodes, id, persister, server, 1000000, batching) // start the kv server. raft recommends 1.2 mb (1000000)
	server.server = rpc.NewServer()
	server.server.RegisterName("Raft", server.Proxy)
	server.server.RegisterName("KVServer", server.Proxy)
	// server.server.RegisterName("Service", server)
	go server.start(wg)
	return server
}

func (server *Server) start(wg *deadlock.WaitGroup) {
	defer wg.Done()
	me, err := server.config.GetAtIndex(server.id)
	if err != nil {
		panic(err)
	}
	// l, err := net.Listen("tcp", me.Address) // For local
	l, err := net.Listen("tcp", ":3001") // For cloud lab
	if err != nil {
		panic(err)
	}
	server.listener = l
	fmt.Println(server.id, " listening on: ", me.Address)
	for !server.shutDown() {
		conn, err := server.listener.Accept()
		if err != nil {
			panic(err)
		}
		go func(conn net.Conn) {
			server.server.ServeConn(conn)
		}(conn)
	}
	fmt.Println(server.id, " exiting now")
	// start listening on the server/
	// send an update to raft to start the election ticker.
}

func (server *Server) shutDown() bool {
	return server.dead.Load()

}

func (server *Server) Kill() {
	server.Proxy.kvServer.Kill()
	server.dead.Store(true)
	fmt.Println(server.id, " received kill", server.dead.Load())
	// go func() {
	// 	time.Sleep(5 * time.Second)
	// 	server.listener.Close()
	// }()
}

// exported RPC functions
// Raft
func (proxy *Proxy) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	proxy.kvServer.rf.RequestVote(args, reply)
	return nil
}

func (proxy *Proxy) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	proxy.kvServer.rf.AppendEntries(args, reply)
	return nil
}

func (proxy *Proxy) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	proxy.kvServer.rf.InstallSnapshot(args, reply)
	return nil
}

// KV Server
func (proxy *Proxy) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	proxy.kvServer.PutAppend(args, reply)
	return nil
}

func (proxy *Proxy) Get(args *GetArgs, reply *GetReply) error {
	proxy.kvServer.Get(args, reply)
	return nil
}

// function to call other functions.

func (server *Server) Call(id int, method string, args, reply interface{}) bool {
	server.rwMu.RLock()
	peer, ok := server.peers[id]
	server.rwMu.RUnlock()
	if !ok {
		p, _ := server.config.GetAtIndex(id)
		client, err := rpc.Dial("tcp", p.Address)
		if err == nil {
			server.rwMu.Lock()
			peer = client
			server.peers[id] = client
			server.rwMu.Unlock()
		} else {
			return false
		}
	}
	err := peer.Call(method, args, reply)
	return err == nil
}

func (proxy *Proxy) GetLeaderId() int {
	return proxy.kvServer.rf.LeaderId
}

func (proxy *Proxy) PrintRfLogs() {
	// for _, v := range proxy.kvServer.rf.Logs {
	// 	fmt.Print(v, ", ")
	// }
	fmt.Println(proxy.kvServer.rf.Logs)
	fmt.Println()
}

func (proxy *Proxy) StartAgreement(command int) {
	index, term, isLeader := proxy.kvServer.rf.Start(command)
	fmt.Println("Command: index, term, isLeader", command, index, term, isLeader)
}

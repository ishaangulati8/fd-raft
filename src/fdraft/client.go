package fdraft

import (
	"time"
	"net/rpc"
)

type Clerk struct {
	servers map[int]*rpc.Client
	me     int64 // my client id
	leader int   // remember which server turned out to be the leader for the last RPC
	opId   int   // operation id, increase monotonically
	config Config

}

func MakeClerk(nodeCount int) *Clerk {
	ck := new(Clerk)
	ck.servers = make(map[int]*rpc.Client)
	ck.config = BuildConfig(nodeCount)
	// You'll have to add code here.
	ck.me = Nrand()
	ck.leader = 0
	ck.opId = 1
	return ck
}

// Fetch the latest value for the key
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{
		Key:      key,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader // send to leader first
	for {
		reply := &GetReply{}
		// ok := ck.servers[serverId].Call("KVServer.Get", args, reply)

		ok := ck.Call(serverId, "KVServer.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			// no reply (reply dropped, network partition, server down, etc.) or
			// wrong leader,
			// try next server
			serverId = (serverId + 1) % len(ck.config.Nodes)
			continue
		}
		if reply.Err == ErrInitElection {
			// sleep for a while, wait for KVServer raft leader election done
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId // remember current leader
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			return reply.Value
		}
	}
}

// Put or append a key
func (ck *Clerk) PutAppend(key string, value string, op opType) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader // send to leader first
	for {
		reply := &PutAppendReply{}
		// ok := ck.servers[serverId].Call("KVServer.PutAppend", args, reply)
		ok := ck.Call(serverId, "KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			// no reply (reply dropped, network partition, server down, etc.) or
			// wrong leader,
			// try next server
			serverId = (serverId + 1) % len(ck.config.Nodes)
			continue
		}
		if reply.Err == ErrInitElection {
			// sleep for a while, wait for KVServer raft leader election done
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId // remember current leader
		if reply.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, opPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, opAppend)
}


func (ck *Clerk) Call(id int, method string, args, reply interface{}) bool{
	node, ok := ck.servers[id]
	if !ok {
		p, _ := ck.config.GetAtIndex(id)
		client, err := rpc.Dial("tcp", p.Address)
		if err == nil {
			ck.servers[id] = client
			node = client
		} else {
			return false
		}
	}
	err := node.Call(method, args, reply)
	return err == nil
}


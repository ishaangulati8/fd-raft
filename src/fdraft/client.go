// package kvraft

// import (
// 	"crypto/rand"
// 	"fmt"
// 	"math/big"
// 	"time"

// 	"6.824/labrpc"
// )

// const RETRY_SLEEP time.Duration = time.Duration(250) * time.Millisecond

// type Clerk struct {
// 	servers []*labrpc.ClientEnd
// 	// You will have to modify this struct.
// 	me     int64
// 	seq    int64
// 	leader int
// }

// func nrand() int64 {
// 	max := big.NewInt(int64(1) << 62)
// 	bigx, _ := rand.Int(rand.Reader, max)
// 	x := bigx.Int64()
// 	return x
// }

// func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
// 	ck := new(Clerk)
// 	ck.servers = servers
// 	// You'll have to add code here.
// 	ck.me = nrand()
// 	ck.seq = 0
// 	ck.leader = 0
// 	return ck
// }

// // fetch the current value for a key.
// // returns "" if the key does not exist.
// // keeps trying forever in the face of all other errors.
// //
// // you can send an RPC with code like this:
// // ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
// //
// // the types of args and reply (including whether they are pointers)
// // must match the declared types of the RPC handler function's
// // arguments. and reply must be passed as a pointer.
// func (ck *Clerk) Get(key string) string {

// 	// You will have to modify this function.
// 	ck.seq += 1
// 	args := GetArgs{
// 		Key:      key,
// 		ClientId: ck.me,
// 		Seq:      ck.seq,
// 	}
// 	leader := ck.leader
// 	for {
// 		reply := new(GetReply)
// 		ok := ck.servers[leader].Call("KVServer.Get", &args, reply)
// 		if !ok || reply.Err == ErrWrongLeader {
// 			leader = (leader + 1) % len(ck.servers)
// 			fmt.Sprintf("Retrying with new leader %d", leader)
// 		} else if reply.Err == ErrNoLeader {
// 			leader = (leader + 1) % len(ck.servers)
// 			time.Sleep(RETRY_SLEEP)
// 		} else {
// 			ck.leader = leader
// 			if reply.Err == ErrNoKey {
// 				return ""
// 			}
// 			return reply.Value
// 		}
// 	}
// 	// return ""
// }

// // shared by Put and Append.
// //
// // you can send an RPC with code like this:
// // ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
// //
// // the types of args and reply (including whether they are pointers)
// // must match the declared types of the RPC handler function's
// // arguments. and reply must be passed as a pointer.
// func (ck *Clerk) PutAppend(key string, value string, op string) {
// 	// You will have to modify this function.
// 	ck.seq += 1
// 	args := PutAppendArgs{
// 		Key:      key,
// 		Value:    value,
// 		Op:       op,
// 		ClientId: ck.me,
// 		Seq:      ck.seq,
// 	}
// 	leader := ck.leader
// 	fmt.Sprintf("trying with new leader %d", leader)
// 	for {
// 		reply := new(PutAppendReply)
// 		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, reply)
// 		if !ok || reply.Err == ErrWrongLeader {
// 			leader = (leader + 1) % len(ck.servers)
// 			fmt.Sprintf("Retrying with new leader %d", leader)
// 		} else if reply.Err == ErrNoLeader {
// 			leader = (leader + 1) % len(ck.servers)
// 			time.Sleep(RETRY_SLEEP)
// 		} else {
// 			ck.leader = leader
// 			return
// 		}
// 	}
// }

// func (ck *Clerk) Put(key string, value string) {
// 	ck.PutAppend(key, value, "Put")
// }
// func (ck *Clerk) Append(key string, value string) {
// 	ck.PutAppend(key, value, "Append")
// }

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

func MakeClerk() *Clerk {
	ck := new(Clerk)
	ck.servers = make(map[int]*rpc.Client)
	ck.config = BuildConfig()
	// You'll have to add code here.
	ck.me = Nrand()
	ck.leader = 0
	ck.opId = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
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

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
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


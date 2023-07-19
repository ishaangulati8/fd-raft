package fdraft

type Err string

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrShutdown     = "ErrShutdown"
	ErrInitElection = "ErrInitElection"
)

type opType string

const (
	opGet    opType = "G"
	opPut    opType = "P"
	opAppend opType = "A"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    opType
	ClientId int64 // id of client
	OpId     int   // client operation id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64 // id of client
	OpId     int   // client operation id
}

type GetReply struct {
	Err   Err
	Value string
}

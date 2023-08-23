package logger

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
// ref: https://blog.josejg.com/debugging-pretty/
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type LogTopic string

const (
	Client           LogTopic = "CLNT"
	Commit           LogTopic = "CMIT"
	Config           LogTopic = "CONF"
	Ctrler           LogTopic = "SCTR"
	Drop             LogTopic = "DROP"
	Error            LogTopic = "ERRO"
	Heart            LogTopic = "HART"
	Info             LogTopic = "INFO"
	Leader           LogTopic = "LEAD"
	Log              LogTopic = "LOG1"
	Log2             LogTopic = "LOG2"
	Migrate          LogTopic = "MIGR"
	Persist          LogTopic = "PERS"
	Snap             LogTopic = "SNAP"
	Server           LogTopic = "SRVR"
	Term             LogTopic = "TERM"
	Test             LogTopic = "TEST"
	Timer            LogTopic = "TIMR"
	Trace            LogTopic = "TRCE"
	Vote             LogTopic = "VOTE"
	Warn             LogTopic = "WARN"
	VoteCandidate    LogTopic = "VOTE_CANDIDATE"
	HeartBeatLeader  LogTopic = "HEART_BEAT_LEADER"
	StaleRpcResponse LogTopic = "STALE_RPC_RESPONSE"
	RaftLock         LogTopic = "RAFT_LOCK"
	RaftUnlock       LogTopic = "RAFT_UNLOCK"
	RaftCondWait     LogTopic = "RAFT_COND_WAIT"
	RaftCondWake     LogTopic = "RAFT_COND_WAKE"
	RaftCondNotify   LogTopic = "RAFT_COND_NOTIFY"
	RaftApply        LogTopic = "RAFT_APPLY"
	ServerReceive    LogTopic = "SERVER_RECEIVE"
	ServerApply      LogTopic = "SERVER_APPLY"
)

var debugStart time.Time
var DebugVerbosity int

func init() {
	DebugVerbosity = getVerbosity()
	debugStart = time.Now()
	// disable datetime logging
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(serverId int, topic LogTopic, format string, a ...interface{}) {
	if (DebugVerbosity == 1 && topic != Info) || DebugVerbosity == 2 {
		// if  DebugVerbosity == 1 &&
		// 	(topic == RaftLock || topic == RaftUnlock || topic == RaftCondWait ||
		// 		topic == RaftCondWake || topic == RaftCondNotify || topic == RaftApply || topic == Heart || topic == Timer ||
		// 		topic == VoteCandidate || topic == HeartBeatLeader ||
		// 		topic == ServerReceive || topic == ServerApply)
		// {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		if serverId >= 0 {
			prefix += fmt.Sprintf("S%d ", serverId)
		}
		format = prefix + format
		log.Printf(format, a...)
	}
}

func ShardDebug(gid int, serverId int, topic LogTopic, format string, a ...interface{}) {
	if DebugVerbosity == 3 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		if gid >= 0 && serverId >= 0 {
			prefix += fmt.Sprintf("G%d S%d ", gid, serverId)
		}
		log.Printf(prefix+format, a...)
	}
}

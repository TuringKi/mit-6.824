package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	FallBehind    = "FallBehind"
)

const (
	GetFlag      = "Get"
	PutFlag      = "Put"
	AppendFlag   = "Append"
	ReconfigFlag = "Reconfig"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Uid string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Uid string
}

type GetReply struct {
	Err   Err
	Value string
}

type ReconfigArgs struct {
	Num    int
	Shards map[int]bool
}

type ReconfigReply struct {
	Data          map[string]string
	RequestNumber map[string]int
	Err           Err
}

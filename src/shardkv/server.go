package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "bytes"
import "reflect"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Type  string
	Key   string
	Value string
	Uid   string
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	shards         map[int]bool
	data           map[string]string
	request_number map[string]int
	cur_seq        int
	encoder        *gob.Encoder
	decoder        *gob.Decoder
	config_num     int
}

// The peer synchronize with others
func (kv *ShardKV) Synchronize(max_seq int) {
	for kv.cur_seq <= max_seq {
		status, value := kv.px.Status(kv.cur_seq)
		//if peer fall behind at seq, restart an instance to catch up
		if status == paxos.Empty {
			kv.startInstance(kv.cur_seq, Op{})
			status, value = kv.px.Status(kv.cur_seq)
		}
		to := 10 * time.Millisecond
		for {
			if status == paxos.Decided {
				kv.encoder.Encode(value)
				var op Op
				kv.decoder.Decode(&op)
				if op.Type != GetFlag && op.Type != ReconfigFlag {
					if _, duplicate := kv.request_number[op.Uid]; !duplicate {
						//fmt.Println("Decided", kv.me, op.Type, op.Key, op.Value)
						switch op.Type {
						case PutFlag:
							kv.data[op.Key] = op.Value
						case AppendFlag:
							_, exist := kv.data[op.Key]
							if exist {
								kv.data[op.Key] += op.Value
							} else {
								kv.data[op.Key] = op.Value
							}
						}
						kv.request_number[op.Uid] = 1
					}
				}
				break
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			status, value = kv.px.Status(kv.cur_seq)
		}
		kv.cur_seq += 1
	}
	kv.px.Done(kv.cur_seq - 1)
}

// start a new instance
func (kv *ShardKV) startInstance(seq int, value Op) interface{} {
	var ans interface{}
	kv.px.Start(seq, value)
	to := 10 * time.Millisecond
	for {
		status, v := kv.px.Status(seq)
		if status == paxos.Decided {
			ans = v
			break
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return ans
}

// Reach a aggreement until there is a log with the value
func (kv *ShardKV) reachAgreement(value Op) {
	for {
		seq := kv.px.Max() + 1
		kv.Synchronize(seq - 1)
		v := kv.startInstance(seq, value)
		if v == value {
			kv.Synchronize(seq)
			break
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	s := key2shard(args.Key)

	reply.Err = OK
	if _, done := kv.request_number[args.Uid]; !done {
		if _, exist := kv.shards[s]; exist {
			op := Op{
				Type:  GetFlag,
				Key:   args.Key,
				Value: "",
				Uid:   args.Uid,
			}
			kv.reachAgreement(op)

			if _, e := kv.data[args.Key]; e {
				reply.Err = OK
				reply.Value = kv.data[args.Key]
			} else {
				reply.Err = ErrNoKey
			}
		} else {
			reply.Err = ErrWrongGroup
		}
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	s := key2shard(args.Key)

	reply.Err = OK
	if _, done := kv.request_number[args.Uid]; !done {
		if _, exist := kv.shards[s]; exist {
			op := Op{
				Type:  args.Op,
				Key:   args.Key,
				Value: args.Value,
				Uid:   args.Uid,
			}

			kv.reachAgreement(op)
		} else {
			reply.Err = ErrWrongGroup
		}
	}
	return nil
}

func (kv *ShardKV) Reconfiguration(args *ReconfigArgs, reply *ReconfigReply) error {
	fmt.Println("start", kv.me, kv.gid)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Println("end", kv.me, kv.gid)

	op := Op{
		Type:  ReconfigFlag,
		Key:   "",
		Value: "",
		Uid:   "",
	}

	kv.reachAgreement(op)

	//update the configuration
	config := kv.sm.Query(args.Num)

	for s, k := range config.Shards {
		_, exist := kv.shards[s]
		if k == kv.gid {
			if !exist {
				kv.shards[s] = true
			}
		} else {
			if exist {
				delete(kv.shards, s)
			}
		}
	}
	kv.config_num = config.Num

	reply.Data = make(map[string]string)
	for k, v := range kv.data {
		s := key2shard(k)
		if _, exist := args.Shards[s]; exist {
			reply.Data[k] = v
		}
	}

	reply.RequestNumber = make(map[string]int)
	for k, v := range kv.request_number {
		reply.RequestNumber[k] = v
	}

	reply.Err = OK
	return nil
}

func (kv *ShardKV) updateConfiguration(num int) {
	old_config := kv.sm.Query(kv.config_num)
	config := kv.sm.Query(num)
	//fmt.Println("config", kv.config_num, config.Num, old_config.Groups)

	if reflect.DeepEqual(config, old_config) {
		return
	}
	new_shards := make(map[int64][]int)
	shards_server := make(map[int64][]string)
	for s, k := range config.Shards {
		_, exist := kv.shards[s]
		if k == kv.gid {
			if !exist {
				//fmt.Println("config", kv.me, kv.gid, kv.config_num, old_config.Groups[k])
				k = old_config.Shards[s]
				if _, ok := old_config.Groups[k]; ok {
					new_shards[k] = append(new_shards[k], s)
					shards_server[k] = old_config.Groups[k]
				}
				kv.shards[s] = true
			}
		} else {
			if exist {
				delete(kv.shards, s)
			}
		}
	}

	kv.config_num = config.Num

	//fmt.Println(kv.me, kv.gid, kv.config_num, kv.shards)
	if len(new_shards) != 0 {
		var args ReconfigArgs
		args.Num = kv.config_num
		for gid, shards := range new_shards {
			args.Shards = make(map[int]bool)
			for _, s := range shards {
				args.Shards[s] = true
			}

			servers := shards_server[gid]
			// try each server in the shard's replication group.
			for _, srv := range servers {
				var reply ReconfigReply
				ok := call(srv, "ShardKV.Reconfiguration", &args, &reply)
				if ok && reply.Err == OK {
					for k, v := range reply.Data {
						kv.data[k] = v
					}
					for k, v := range reply.RequestNumber {
						kv.request_number[k] = v
					}
					break
				}
			}
		}
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.updateConfiguration(-1)

}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	kv.shards = make(map[int]bool)
	kv.data = make(map[string]string)
	kv.request_number = make(map[string]int)
	var network bytes.Buffer
	kv.encoder = gob.NewEncoder(&network)
	kv.decoder = gob.NewDecoder(&network)
	kv.cur_seq = 0
	kv.config_num = -1

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}

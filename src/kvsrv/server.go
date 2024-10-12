package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Response struct {
	requestId int64
	value     string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store     map[string]string
	responses map[int64]Response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value := kv.store[args.Key]
	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// detect duplicate
	response := kv.detectDuplicate(args.ClientId, args.RequestId)
	if response != nil {
		return
	}

	kv.store[args.Key] = args.Value

	// save request value
	kv.responses[args.ClientId] = Response{args.RequestId, ""}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// detect duplicate
	response := kv.detectDuplicate(args.ClientId, args.RequestId)
	if response != nil {
		reply.Value = *response
		return
	}

	value := kv.store[args.Key]
	kv.store[args.Key] += args.Value
	reply.Value = value

	// save request value
	kv.responses[args.ClientId] = Response{args.RequestId, value}
}

func (kv *KVServer) detectDuplicate(clientId, requestId int64) *string {
	if response, ok := kv.responses[clientId]; ok {
		if response.requestId >= requestId {
			// duplicate detected
			return &response.value
		} else {
			// delete old responses
			delete(kv.responses, clientId)
		}
	}
	return nil
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.responses = make(map[int64]Response)

	return kv
}

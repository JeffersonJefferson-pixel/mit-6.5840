package kvsrv

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	Id        int64
	RequestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.Id = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// increment request id
	reqId := atomic.AddInt64(&ck.RequestId, 1)
	args := GetArgs{key, ck.Id, reqId}

	for {
		reply := GetReply{}
		if ok := ck.server.Call("KVServer.Get", &args, &reply); ok {
			return reply.Value
		}
		// retry
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	// increment request id
	reqId := atomic.AddInt64(&ck.RequestId, 1)

	args := PutAppendArgs{key, value, ck.Id, reqId}

	for {
		reply := PutAppendReply{}
		if ok := ck.server.Call("KVServer."+op, &args, &reply); ok {
			return reply.Value
		}
		// retry
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

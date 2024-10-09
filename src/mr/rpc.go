package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type GetTaskArgs struct {
}

type GetTaskReply struct {
	Index     int
	Input     string
	NumReduce int
	Type      TaskType
}

type CompleteTaskArgs struct {
	Index int
	Type  TaskType
}

type CompleteTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

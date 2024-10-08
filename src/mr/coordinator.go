package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks    []MapTask
	ReduceTasks []ReduceTask

	mu sync.Mutex
}

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
)

type MapTask struct {
	Id    int
	File  string
	State TaskState
}

type ReduceTask struct {
	Id     int
	State  TaskState
	Worker int
}

type TaskState int

const (
	IDLE TaskState = iota
	IN_PROGRESS
	COMPLETED
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// find idle or in-progress task in map
	for _, task := range c.MapTasks {
		if task.State == IDLE {
			fmt.Printf("assigning task %v of type %v!\n", task.Id, MAP)
			reply.Filename = task.File
			reply.Id = task.Id
			reply.NumReduce = len(c.ReduceTasks)
			reply.Type = MAP

			task.State = IN_PROGRESS

			return nil
		}
	}

	// find idle or in-progress reduce task in reduce
	for _, task := range c.ReduceTasks {
		if task.State == IDLE {
			fmt.Printf("assigning task %v of type %v!\n", task.Id, REDUCE)
			reply.Id = task.Id
			reply.NumMap = len(c.MapTasks)
			reply.Type = REDUCE

			task.State = IN_PROGRESS

			return nil
		}
	}

	reply.Id = -1

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("completing task %v of type %v!\n", args.Id, args.Type)

	if args.Type == MAP {
		c.MapTasks[args.Id].State = COMPLETED
	} else {
		c.ReduceTasks[args.Id].State = COMPLETED
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	for _, task := range c.MapTasks {
		if task.State != COMPLETED {
			return false
		}
	}

	for _, task := range c.ReduceTasks {
		if task.State != COMPLETED {
			return false
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// map
	c.MapTasks = make([]MapTask, len(files))
	for i := 0; i < len(files); i++ {
		c.MapTasks[i] = MapTask{
			Id:   i,
			File: files[i],
		}
	}

	// reduce
	c.ReduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = ReduceTask{
			Id: i,
		}
	}

	c.server()
	return &c
}

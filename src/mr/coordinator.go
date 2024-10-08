package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
)

type TaskState int

const (
	IDLE TaskState = iota
	IN_PROGRESS
	COMPLETED
)

type Task struct {
	Id        int
	Input     string
	ReduceNum int
	State     TaskState
	Type      TaskType
	StartedAt time.Time
}

type Coordinator struct {
	// Your definitions here.
	NumMap    int
	NumReduce int

	Tasks []Task

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// find idle or in-progress task
	for i, task := range c.Tasks {
		if task.State == IDLE {
			fmt.Printf("assigning task %v of type %v!\n", task.Id, task.Type)
			reply.Input = task.Input
			reply.Id = task.Id
			reply.NumMap = c.NumMap
			reply.NumReduce = c.NumReduce
			reply.Type = task.Type
			reply.ReduceNum = task.ReduceNum

			c.Tasks[i].State = IN_PROGRESS

			go c.waitForTask(task.Id)

			return nil
		}
	}

	reply.Id = -1

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("completing task %v of type %v!\n", args.Id, c.Tasks[args.Id].Type)

	c.Tasks[args.Id].State = COMPLETED

	return nil
}

func (c *Coordinator) waitForTask(id int) {
	time.Sleep(10 * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Tasks[id].State == IN_PROGRESS {
		c.Tasks[id].State = IDLE
	}
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

	for _, task := range c.Tasks {
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
	c.NumMap = len(files)
	c.NumReduce = nReduce

	// tasks
	c.Tasks = []Task{}
	for i, file := range files {
		c.Tasks = append(c.Tasks, Task{
			Id:    i,
			Input: file,
			Type:  MAP,
		})
	}

	// reduce
	for i := 0; i < nReduce; i++ {
		c.Tasks = append(c.Tasks, Task{
			Id:        c.NumMap + i,
			ReduceNum: i,
			Type:      REDUCE,
		})
	}

	c.server()
	return &c
}

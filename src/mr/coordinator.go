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

const TaskTimeout = 10 * time.Second

type TaskType int

const (
	Map TaskType = iota
	Reduce
	NoTask
	Exit
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	Index int
	Input string
	State TaskState
	Type  TaskType
}

type Coordinator struct {
	// Your definitions here.
	NumMap    int
	NumReduce int

	MapTasks    []Task
	ReduceTasks []Task

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	// find idle task
	if c.NumMap > 0 {
		task = c.selectTask(c.MapTasks)
	} else if c.NumReduce > 0 {
		task = c.selectTask(c.ReduceTasks)
	} else {
		task = &Task{-1, "", Completed, Exit}
	}
	reply.Index = task.Index
	reply.Input = task.Input
	reply.NumReduce = len(c.ReduceTasks)
	reply.Type = task.Type

	go c.waitForTask(task)

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == Map {
		if c.MapTasks[args.Index].State == InProgress {
			c.MapTasks[args.Index].State = Completed
			c.NumMap--
		}
	} else if args.Type == Reduce {
		if c.ReduceTasks[args.Index].State == InProgress {
			c.ReduceTasks[args.Index].State = Completed
			c.NumReduce--
		}
	} else {
		fmt.Printf("Incorrect task type to complete: %v\n", args.Type)
	}

	return nil
}

func (c *Coordinator) selectTask(tasks []Task) *Task {
	var task *Task
	for i := 0; i < len(tasks); i++ {
		task = &tasks[i]
		if task.State == Idle {
			task.State = InProgress

			return task
		}
	}

	return &Task{-1, "", Completed, NoTask}
}

func (c *Coordinator) waitForTask(task *Task) {
	if task.Type != Map && task.Type != Reduce {
		return
	}

	<-time.After(TaskTimeout)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.State == InProgress {
		task.State = Idle
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
	// Your code here.

	return c.NumMap == 0 && c.NumReduce == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NumMap = len(files)
	c.NumReduce = nReduce

	// map tasks
	c.MapTasks = make([]Task, 0, c.NumMap)
	for i, file := range files {
		mapTask := Task{
			Index: i,
			Input: file,
			Type:  Map,
		}
		c.MapTasks = append(c.MapTasks, mapTask)
	}

	// reduce tasks
	c.ReduceTasks = make([]Task, 0, c.NumReduce)
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{
			Index: i,
			Type:  Reduce,
		}
		c.ReduceTasks = append(c.ReduceTasks, reduceTask)
	}

	c.server()
	return &c
}

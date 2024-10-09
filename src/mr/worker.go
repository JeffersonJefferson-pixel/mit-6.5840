package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const TaskInterval = time.Millisecond * 200

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		// get task
		reply := CallGetTask()

		// handle task
		var success bool
		if reply.Type == Map {
			success = HandleMapTask(mapf, reply)
		} else if reply.Type == Reduce {
			success = HandleReduceTask(reducef, reply)
		} else if reply.Type == Exit {
			return
		}

		// do nothing on no task

		// complete task
		if success {
			CallCompleteTask(reply.Index, reply.Type)
		}

		time.Sleep(TaskInterval)
	}
}

func HandleMapTask(mapf func(string, string) []KeyValue, reply *GetTaskReply) bool {
	// read input
	content := ReadInput(reply.Input)

	// call map function
	kva := mapf(reply.Input, content)

	// task number -> list of key values
	group := make(map[int][]KeyValue)

	for _, kv := range kva {
		// hash key
		reduceId := ihash(kv.Key) % reply.NumReduce
		group[reduceId] = append(group[reduceId], kv)
	}

	for reduceId, kva := range group {
		// write to intermediate file
		iname := fmt.Sprintf("mr-%d-%d", reply.Index, reduceId)
		ifile, _ := os.Create(iname)

		enc := json.NewEncoder(ifile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}

		ifile.Close()
	}

	return true
}

func HandleReduceTask(reducef func(string, []string) string, reply *GetTaskReply) bool {
	// read intemediates
	intermediate := ReadIntermediate(reply.Index)

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.Index)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return true
}

func CallGetTask() *GetTaskReply {
	args := GetTaskArgs{}

	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("cannot get task!\n")
		return nil
	}
}

func CallCompleteTask(taskIndex int, taskType TaskType) *CompleteTaskReply {
	args := CompleteTaskArgs{
		Index: taskIndex,
		Type:  taskType,
	}

	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("cannot complete task %v of type %v!\n", taskIndex, taskType)
		return nil
	}
}

func ReadInput(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func ReadIntermediate(reduceId int) []KeyValue {
	kva := []KeyValue{}
	// get all intermediate filenames of the reduce task
	filenames, err := filepath.Glob(fmt.Sprintf("mr-*-%d", reduceId))
	if err != nil {
		log.Printf("cannot read intermediate files of reduce id %v", reduceId)
		return nil
	}
	for _, filename := range filenames {
		// read intermediate file
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		// decode
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	return kva
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

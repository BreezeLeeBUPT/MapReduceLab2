package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Status int
const StatusNotStart Status = 0
const StatusWorking Status = 1
const StatusFinish Status = 2

type Coordinator struct {
	// Your definitions here.
	files []string
	mapStatus []Status
	mapFinishCnt int32
	reduceStatus []Status
	reduceFinishCnt int32
	nReduce int
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 5
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// metadata
	reply.NReduce = c.nReduce
	reply.FileCnt = len(c.files)

	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.IsFinishMap() {
		for idx, status := range c.mapStatus {
			if status == StatusNotStart {
				reply.Task = TaskTypeMap
				reply.FileName = c.files[idx]
				reply.TaskNum = idx
				c.mapStatus[idx] = StatusWorking
				// handle timeout
				go func(idx int) {
					time.Sleep(10 * time.Second)
					if c.reduceStatus[idx] != StatusFinish {
						c.reduceStatus[idx] = StatusNotStart
					}
				}(idx)
				return nil
			}
		}
		// no available job currently
		reply.Task = TaskTypeWait
	} else if !c.IsFinishReduce() {
		for idx, status := range c.reduceStatus {
			if status == StatusNotStart {
				reply.Task = TaskTypeReduce
				reply.NReduce = c.nReduce
				reply.TaskNum = idx
				c.reduceStatus[idx] = StatusWorking
				// handle timeout
				go func(idx int) {
					time.Sleep(10 * time.Second)
					if c.reduceStatus[idx] != StatusFinish {
						c.reduceStatus[idx] = StatusNotStart
					}
				}(idx)
				return nil
			}
		}
		reply.Task = TaskTypeWait
	} else {
		// all done
		reply.Task = TaskTypeNone
	}
	return nil
}

func (c *Coordinator) Finish(args *FinishArg, reply *FinishArg) error {
	if !c.IsFinishMap() {
		c.mapStatus[args.TaskNum] = StatusFinish
		atomic.AddInt32(&c.mapFinishCnt, 1)
		fmt.Printf("map task %v finished, map status=%v\n", args.TaskNum, c.mapStatus)
	} else if !c.IsFinishReduce(){
		c.reduceStatus[args.TaskNum] = StatusFinish
		atomic.AddInt32(&c.reduceFinishCnt, 1)
		fmt.Printf("reduce task %v finished, reduce status=%v\n", args.TaskNum, c.reduceStatus)
	}
	return nil
}

func (c *Coordinator) IsFinishMap() bool {
	return int(c.mapFinishCnt) == len(c.files)
}


func (c *Coordinator) IsFinishReduce() bool {
	return int(c.reduceFinishCnt) == c.nReduce
}
//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.IsFinishReduce()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = make([]string, len(files))
	copy(c.files, files)
	c.nReduce = nReduce

	c.mapStatus = make([]Status, len(files))
	c.reduceStatus = make([]Status, nReduce)

	c.server()
	return &c
}

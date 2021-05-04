package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type taskType int
const TaskTypeMap taskType = 1
const TaskTypeReduce taskType = 2
const TaskTypeWait taskType = 3
const TaskTypeNone taskType = 4


type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task taskType
	FileName string
	TaskNum int // may be map task num, or reduce task num
	NReduce int
	FileCnt int // used in reduce field, to get name of intermediate file
}

type FinishArg struct {
	TaskNum int
}

type FinishReply struct {
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

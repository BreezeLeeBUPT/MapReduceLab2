package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// declare an argument structure.
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	fmt.Printf("reply=%+v\n", reply)
	if reply.Task == TaskTypeMap {
		f, err := os.Open(reply.FileName)
		if err != nil {
			fmt.Printf("open file %v failed, err=%v", reply.FileName, err)
			return
		}
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			fmt.Printf("read failed, err=%v", err)
			return
		}
		kva := mapf(reply.FileName, string(bytes))
		intermediate := make([]KeyValue, 0)
		intermediate = append(intermediate, kva...)
		sort.Sort(ByKey(intermediate))
		files := make([]*os.File, reply.NReduce)
		inputs := make([]*json.Encoder, reply.NReduce)
		for i := 0; i < reply.NReduce; i++ {
			mediumFileName := fmt.Sprintf("mr-%v-%v", reply.TaskNum, i)
			//_, err := os.Stat(mediumFileName)
			//if os.IsNotExist(err) {
			//	files[i], _ = os.Create(mediumFileName)
			//} else {
			//	files[i], _ = os.Open(mediumFileName)
			//}
			files[i], _ = os.Create(mediumFileName)
			inputs[i] = json.NewEncoder(files[i])
		}
		for _, kv := range intermediate {
			reduceTaskNum := ihash(kv.Key) % reply.NReduce
			err := inputs[reduceTaskNum].Encode(&kv)
			if err != nil {
				fmt.Printf("encode failed, err=%v", err)
			}
		}
		call("Coordinator.Finish", FinishArg{TaskNum: reply.TaskNum}, FinishReply{})
	} else if reply.Task == TaskTypeReduce {
		kva := make([]KeyValue, 0)
		for i := 0; i < reply.FileCnt; i++ {
			mediumFileName := fmt.Sprintf("mr-%v-%v", i, reply.TaskNum)
			//_, err := os.Stat(mediumFileName)
			var file *os.File
			//if os.IsNotExist(err) {
			//	file, _ = os.Create(mediumFileName)
			//} else {
			//	file, _ = os.Open(mediumFileName)
			//}
			file, _ = os.Create(mediumFileName)
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
		sort.Sort(ByKey(kva))

		oname := fmt.Sprintf("mr-out-%v", reply.TaskNum)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}


		call("Coordinator.Finish", FinishArg{TaskNum: reply.TaskNum}, FinishReply{})
	} else if reply.Task == TaskTypeWait {
		Worker(mapf, reducef)
	} else if reply.Task == TaskTypeNone {
		fmt.Println("finished.")
	}
	fmt.Printf("reply.Y %v\n", reply.Task)
	fmt.Printf("reply.FileName %v\n", reply.FileName)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

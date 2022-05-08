package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
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
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	workId string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	alive := true
	for alive {
		args := ExampleArgs{}
		job := Job{}
		call("Coordinator.DistributeJob", &args, &job)
		switch job.JobType {
		case MapJob:
			fmt.Println(workId, " gain a map job")
			DoMap(mapf, &job)
			reply := ExampleReply{}
			call("Coordinator.JobIsDone", &job, &reply)
		case ReduceJob:
			fmt.Println(workId, " gain a reduce job")
			DoReduce(reducef, &job)
			reply := ExampleReply{}
			call("Coordinator.JobIsDone", &job, &reply)
		case WaittingJob:
			fmt.Println(workId, " gain a waiting job")
			time.Sleep(time.Second)
		case KillJob:
			fmt.Println(workId, " gain a kill job")
			time.Sleep(time.Second)
			alive = false
		}
		time.Sleep(time.Second)
	}
}

func DoMap(mapf func(string, string) []KeyValue, response *Job) {
	fmt.Println("start do map job:", response.JobId)
	var intermediate []KeyValue
	//filename := "./main/" + response.InputFile[0]
	filename := response.InputFile[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))
	rn := response.ReducerNum
	Hashedkv := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		Hashedkv[ihash(kv.Key)%rn] = append(Hashedkv[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.JobId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range Hashedkv[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
	fmt.Println("done map job:", response.JobId)
}

func DoReduce(reducef func(string, []string) string, response *Job) {
	fmt.Println("start do reduce job:", response.JobId)
	reduceFileNum := response.JobId
	intermediate := readFromLocalFile(response.InputFile)
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	ofile, _ := os.Create(oname)
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
	fmt.Println("done reduce job:", response.JobId, oname)
}
func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		fmt.Println(filepath)
		file, _ := os.Open(filepath)
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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

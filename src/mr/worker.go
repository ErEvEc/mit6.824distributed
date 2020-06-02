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

// for sorting by key, define a type
type ByKey []KeyValue
func (a ByKey) Len() int {return len(a)}
func (a ByKey) Swap(i, j int) {a[i], a[j] = a[j], a[i]}
func (a ByKey) Less(i, j int) bool {return a[i].Key < a[j].Key}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func DealMap(mapf func(string, string) []KeyValue, mapstruct MapReply) map[int][]string {
	file, err := os.Open(mapstruct.Filename)
	if err != nil {
		log.Fatalf("cannot open: #{file}")
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read: #{mapstruct.Filename}") // can you parse the variable
	}
	file.Close()

	// intermediate k-v pairs/ key value array
	kva := mapf(mapstruct.Filename, string(content))

	// make same hash cod eto one map array
	hashmaps := make(map[int][]KeyValue)
	for _, kv := range kva {
		r := ihash(kv.Key) % mapstruct.NReduce
		hashmaps[r] = append(hashmaps[r], kv)
	}

	// hashcode to filename
	interfiles := make(map[int][]string)
	for k, v := range hashmaps{
		filename := "mr-" + strconv.Itoa(mapstruct.Tasknumber) + "-" + strconv.Itoa(k+1)
		os.Remove(filename)

		file, er := os.Create(filename)
		if er != nil {
			log.Fatalf("cannot create: #{filename}")
		}

		// here kv should only be the value
		enc := json.NewEncoder(file)
		for _, kv := range v {
			er = enc.Encode(&kv)
			if er != nil {
				log.Fatalf("cannot encode: #{filename}")
			}

		}

		// aggregate same key from different files
		interfiles[k] = append(interfiles[k], filename)
		file.Close()
	}

	return interfiles
}

func DealReduce(reducef func(string, []string) string, reducestruct ReduceReply) string {
	var kva []KeyValue

	// always start from read
	for _, filename := range reducestruct.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open: #{filename}")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// a while loop to decode file
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}
	log.Printf("dealreduce:#{reducestruct.Files}\n")
	sort.Sort(ByKey(kva)) // 相当于传进去一个comparator

	oname := "mr-out-" + strconv.Itoa(reducestruct.Tasknumber + 1)
	os.Remove(oname)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in kva[],
	i := 0
	for i<len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}

		for k:=i; k<j; k++ {
			values = append(values, kva[k].Value)
		}
		//here get the combined value
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n",kva[i].Key, output)

		i = j

	}

	ofile.Close()

	return oname

}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	// map phase
	for {
		mapargs := MapTaskArgs{}
		mapargs.Maptask = true
		mapreply := MapReply{}

		call("Master.AssignMap", &mapargs, &mapreply)

		if mapreply.Flag == 1 {
			interfiles := DealMap(mapf, mapreply)

			endmapargs := EndMapArgs{mapreply.Tasknumber, interfiles}
			endmapreply := EndMapReply{}

			res := call("Master.CommitMediateFiles", &endmapargs, &endmapreply)
			log.Printf("master commitmediatefiles res: %v\n", res)
			if endmapreply.Err != nil {
				log.Fatalf("commit intermediate file failed, tn : #{mapreply.Tasknumber}")
			}
		} else if mapreply.Flag == 2 {
			time.Sleep(10 * time.Millisecond)
		} else if mapreply.Flag == 3 {
			log.Printf("all map tasks end,begin to reduce:%v\n", mapreply.Flag)
			break
		} else {
			log.Printf("Request for map task failue:%v\n", mapreply.Flag)
			break
		}
	}

	// reduce phase
	for {
		reduceargs := MapTaskArgs{false}
		reducereply := ReduceReply{}

		call("Master.AssignReduce", &reduceargs, &reducereply)

		if reducereply.Flag == 1 {
			filename := DealReduce(reducef, reducereply)
			reportreduceargs := ReportReduceArgs{reducereply.Tasknumber, filename}
			reportreducereply := ReportReduceReply{}

			call("Master.CommitFinalFiles", &reportreduceargs, &reportreducereply)
			if reportreducereply.Err != nil {
				log.Fatalf("commit final file failure task num: #{reducereply.Tasknumber}")
			}
		}else if reducereply.Flag == 2 {
			time.Sleep(10 * time.Millisecond)
		}else if reducereply.Flag == 3 {
			log.Printf("all reduce tasks end\n")
			break
		} else {
			log.Printf("request for reduce failuere: #{reducereply.Flag}\n")
			break
		}
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

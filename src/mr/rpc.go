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

// Add your RPC definitions here.
// true - map; false - reduce
type MapTaskArgs struct {
	Maptask bool
}

// flag: 0 - err; 1 - begin map;
// 2 - not map task temporarily, wait
// 3 -  all end, move to reduce
type MapReply struct {
	Filename string
	Tasknumber int
	NReduce int
	Flag byte
}

type EndMapArgs struct {
	Tasknumber int
	Files map[int][]string
}

type EndMapReply struct {
	Err error
}

// flag: 0 - err; 1 - begin reduce;
// 2 - not reduce task temporarily, wait
// 3 -  all end, final
type ReduceReply struct {
	Files []string
	Tasknumber int
	Flag byte
}

type ReportReduceArgs struct {
	Tasknumber int
	Filename string // final outpu only one file
}

type ReportReduceReply struct {
	Err error
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// 之前没有这个的 多了一个unique name  其他都一样
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

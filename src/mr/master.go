package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	mu sync.Mutex

	nReduce	int
	leftmap int
	leftreduce int

	// unused files record
	inputfileset []bool
	receivemapset []bool

	// unused mediatefiles
	hashset map[int]bool
	hashsetbk map[int]bool

	// crash: [tasknumber]unix time
	mapcrash map[int]int64
	reducecrash map[int]int64

	// indexinput
	inputfiles []string
	mediatefiles map[int][]string
	finalfiles []string

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// imitate Example above to process rpc call
func (m *Master) AssignMap(args *MapTaskArgs, reply *MapReply) error {
	if args.Maptask == false {
		reply.Flag = 0
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// flag type 3
	if m.leftmap <= 0 {
		reply.Flag = 3
		return nil
	}

	for k, v := range m.inputfileset {
		// flag type 1
		if v == false {
			// find one that is unused
			reply.Filename = m.inputfiles[k] // k is index
			reply.Tasknumber = k
			reply.NReduce = m.nReduce // what is this?
			reply.Flag = 1
			m.inputfileset[k] = true // this is also this loop's condition
			m.mapcrash[k] = time.Now().Unix()
			return nil
		}
	}

	// flag type 2
	reply.Flag = 2
	return nil
}

func (m *Master) CommitMediateFiles(args *EndMapArgs, reply *EndMapReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tn := args.Tasknumber

	//have not received this MediateFile, why print at the beginning?

	if m.receivemapset[tn] == false {
		log.Printf("tasktnumber: %v, master receive: %v\n", tn, m.receivemapset[tn])
		for k, v := range args.Files {
			m.mediatefiles[k] = append(m.mediatefiles[k], v...)
			m.hashset[k] = true // what this boolean for?
		}
		m.receivemapset[tn] = true
		m.inputfileset[tn] = true // already set true in Assigning why duplicate here?
		m.leftmap--

		// check if all map tasks end
		if m.leftmap <= 0 {
			for k, v := range m.hashset {
				m.hashsetbk[k] = v // bk means break
				// 相当于copy 了整个true的boolean map ？
			}
			m.leftreduce = len(m.hashsetbk)
			log.Printf("CommitMediateFiles end\n")
		}
		return nil
	}
	log.Printf("tasknumber: %v has already received\n", tn)
	return nil
}

func (m *Master) AssignReduce(args *MapTaskArgs, reply *ReduceReply) error  {
	if args.Maptask == true {
		reply.Flag = 0
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// flag type 3
	if m.leftreduce <= 0 {
		reply.Flag = 3
		return nil
	}

	for k, v := range m.hashset {
		// flag type 1
		// find an unused like above
		if v == true {
			reply.Tasknumber = k
			reply.Files = m.mediatefiles[k]
			reply.Flag = 1
			m.hashset[k] = false // alter this loop's condition
			m.reducecrash[k] = time.Now().Unix()
			return nil
		}
	}

	// flag type 2
	reply.Flag = 2
	return nil
}


func (m *Master) CommitFinalFiles(args *ReportReduceArgs, reply *ReportReduceReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tn := args.Tasknumber
	// prevent duplicate commit
	if m.hashsetbk[tn] == true {
		m.finalfiles = append(m.finalfiles, args.Filename)
		m.hashsetbk[tn] = false
		m.hashset[tn] = false // why both, at first why bother?
		m.leftreduce--
	}
	return nil
}

func (m *Master) EndDealCrash() bool {
	ret := false
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.leftmap > 0 {
		nowunixtime := time.Now().Unix()
		for k, v := range m.mapcrash {
			if nowunixtime - v > 5 && m.receivemapset[k] == false {
				log.Printf("enddealcrash map tasknumber: %v, now: %v, last: %v\n",
					k, nowunixtime, v)
				m.inputfileset[k] = false // reset to unused

			}
		}
	} else if m.leftreduce > 0 {
		nowunixtime := time.Now().Unix()
		for k, v := range m.reducecrash {
			if nowunixtime-v > 5 && m.hashsetbk[k] == true {
				log.Printf("enddealcrash reduce tasknumber: %v, now: %v, last: %v\n",
					k, nowunixtime, v)
				m.hashset[k] = false // reset to unused why here is only one?

			}
		}
	} else {
		ret = true
	}

	return ret
}

func (m *Master) ScheduleDlCrash() {
	time.Sleep(5 * time.Second)
	for m.EndDealCrash() == false {
		time.Sleep(5 * time.Second)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// this place is a little bit different
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	// even in check whether done need mutex
		if len(m.hashsetbk) > 0 {
			if m.leftreduce <= 0 {
				log.Printf("master done\n")
				ret = true
			}

		}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.inputfiles = files
	m.nReduce = nReduce
	m.hashset = make(map[int]bool)
	m.hashsetbk = make(map[int]bool)
	m.mediatefiles = make(map[int][]string)

	m.leftmap = len(files)

	m.inputfileset = make([]bool, len(files))
	m.receivemapset = make([]bool, len(files))

	m.mapcrash = make(map[int]int64)
	m.reducecrash = make(map[int]int64)

	m.server()
	go m.ScheduleDlCrash()

	log.Printf("Init master Success!\n")

	return &m

	m.server()
	return &m
}

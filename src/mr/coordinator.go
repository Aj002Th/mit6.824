package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskInfo struct {
}

type Coordinator struct {
	// Your definitions here.

	// 生命周期控制
	ControlLock sync.Mutex
	NMap        int        // map总任务数
	NReduce     int        // reduce总任务数
	TaskList    []TaskInfo // task表
	Stage       int        // coordinator阶段

	// map 任务
	MapLock     sync.Mutex
	MapNeed     int // 未完成 map task 数目
	MapTaskChan chan Task

	// reduce 任务
	ReduceLock     sync.Mutex
	ReduceNeed     int // 未完成 reduce task 数目
	ReduceTaskChan chan Task
}

// Your code here -- RPC handlers for the worker to call.

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *Task, reply *Task) error {
	return nil
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	//taskList := make([]TaskInfo, 0)
	//for i, fname := range files {
	//	taskInfo := TaskInfo {
	//
	//	}
	//}

	c := Coordinator{
		NMap:    nMap,
		NReduce: nReduce,
	}

	c.server()
	return &c
}

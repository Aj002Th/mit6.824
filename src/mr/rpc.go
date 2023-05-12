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

// Task 通信结构
type Task struct {
	TaskType    int
	TaskID      int
	InputFiles  []string
	OutputFiles []string
	NReduce     int
}

// Empty 通信结构
type Empty struct{}

// coordinator 状态枚举量
const (
	MapStage int = iota
	ReduceStage
	AllDoneStage
)

// task 类型枚举量
const (
	MapTask int = iota
	ReduceTask
	WaitTask
	ExitTask
)

// task 状态枚举量
const (
	Waiting int = iota
	Working
	Done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

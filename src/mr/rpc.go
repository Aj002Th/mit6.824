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

// 通信结构
type Task struct {
	TaskType int
	TaskID int
	InputFiles []string
	OutputFiles []string
	NReduce int
}

type Empty struct {}

// 一些枚举量
const (
	// coordinator 状态
	MapStage int = iota
	ReduceStage
	AllDoneStage
)

const (
	// task 类型
	MapTask int = iota
	ReduceTask
	WaitTask
	ExitTask
)

const (
	// task 状态
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

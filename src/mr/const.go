package mr

import "time"

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

const Timeout time.Duration = 8 * time.Second

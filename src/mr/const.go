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

// 时间量
const (
	Timeout          time.Duration = 10 * time.Second
	WaitTaskDuration time.Duration = 500 * time.Millisecond
	RpcDuration      time.Duration = 100 * time.Millisecond
)

// Debug 是否开启日志
const Debug = true

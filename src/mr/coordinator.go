package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// TaskInfo task 全量信息
type TaskInfo struct {
	TaskType   int
	TaskID     int
	InputFiles []string
	TaskStatus int
}

// TaskToken task 标识信息
type TaskToken struct {
	TaskType int
	TaskID   int
}

type Coordinator struct {
	// Your definitions here.

	// 生命周期控制
	ControlLock sync.Mutex
	NMap        int                 // map总任务数
	NReduce     int                 // reduce总任务数
	TaskList    map[string]TaskInfo // task表
	Stage       int                 // coordinator阶段

	// map 任务
	MapLock     sync.Mutex
	MapNeed     int // 未完成 map task 数目
	MapTaskChan chan TaskToken

	// reduce 任务
	ReduceLock     sync.Mutex
	ReduceNeed     int // 未完成 reduce task 数目
	ReduceTaskChan chan TaskToken
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

func (c *Coordinator) GetTask(args *Empty, reply *Task) error {
	switch c.Stage {
	case MapStage:
		taskToken, ok := <-c.MapTaskChan
		// chan 里没任务了 - 等待 map 任务全部执行结束
		if !ok {
			reply.TaskType = WaitTask
			break
		}
		// 修改任务状态
		c.ControlLock.Lock()
		taskKey := getMapTaskKey(taskToken.TaskID)
		taskInfo, ok := c.TaskList[taskKey]
		if !ok {
			log.Printf("[GetTask - map] Task %v not found", taskToken.TaskID)
			reply.TaskType = WaitTask
			break
		}
		if taskInfo.TaskStatus == Done {
			// 走入这个分支的情景: 任务超时后重新被放入 chan, 还没下发原先执行该任务的 worker 就完成了任务
			// 忽略掉这个任务即可
			log.Printf("[GetTask - map] Task %v have been done", taskToken.TaskID)
			reply.TaskType = WaitTask
			break
		}
		taskInfo.TaskStatus = Working
		c.TaskList[taskKey] = taskInfo
		// 组装 reply 信息
		c.getTaskFromInfo(reply, taskInfo)
		c.ControlLock.Unlock()

	case ReduceStage:
		taskToken, ok := <-c.ReduceTaskChan
		// chan 里没任务了 - 等待 reduce 任务全部执行结束
		if !ok {
			reply.TaskType = WaitTask
			break
		}
		// 修改任务状态
		c.ControlLock.Lock()
		taskKey := getReduceTaskKey(taskToken.TaskID)
		taskInfo, ok := c.TaskList[taskKey]
		if !ok {
			log.Printf("[GetTask - reduce] Task %v not found", taskToken.TaskID)
			reply.TaskType = WaitTask
			break
		}
		if taskInfo.TaskStatus == Done {
			// 走入这个分支的情景: 任务超时后重新被放入 chan, 还没下发原先执行该任务的 worker 就完成了任务
			// 忽略掉这个任务即可
			log.Printf("[GetTask - reduce] Task %v have been done", taskToken.TaskID)
			reply.TaskType = WaitTask
			break
		}
		taskInfo.TaskStatus = Working
		c.TaskList[taskKey] = taskInfo
		// 组装 reply 信息
		c.getTaskFromInfo(reply, taskInfo)
		c.ControlLock.Unlock()

	case AllDoneStage:
		reply.TaskType = ExitTask
	}
	return nil
}

func (c *Coordinator) FinishTask(args *Task, reply *Empty) error {
	switch args.TaskType {
	case MapStage:
		c.MapLock.Lock()
		c.MapLock.Unlock()
	case ReduceStage:
		c.ReduceLock.Lock()
		c.ReduceLock.Unlock()
	default:
	}
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
	c.ReduceLock.Lock()
	defer c.ReduceLock.Unlock()
	if c.ReduceNeed == 0 {
		ret = true
	}

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	taskList := make(map[string]TaskInfo, 0)

	// 创建 map 任务
	mapTaskChan := make(chan TaskToken, nMap)
	for i, fname := range files {
		// 填充 task list
		taskKey := getMapTaskKey(i)
		taskInfo := TaskInfo{
			TaskType:   MapTask,
			TaskID:     i,
			InputFiles: []string{fname},
			TaskStatus: Waiting,
		}
		taskList[taskKey] = taskInfo

		// 填充 task chan
		mapTaskChan <- TaskToken{
			TaskType: MapTask,
			TaskID:   i,
		}
	}

	// 创建 reduce 任务
	reduceTaskChan := make(chan TaskToken, nMap)
	for i := 0; i < nReduce; i++ {
		inputFiles := make([]string, 0)
		for j := 0; j < nMap; j++ {
			inputFiles = append(inputFiles, fmt.Sprintf("mr-%d-%d", j, i))
		}

		// 填充 task list
		taskKey := getReduceTaskKey(i)
		taskInfo := TaskInfo{
			TaskType:   ReduceTask,
			TaskID:     i,
			InputFiles: inputFiles,
			TaskStatus: Waiting,
		}
		taskList[taskKey] = taskInfo

		// 填充 task chan
		reduceTaskChan <- TaskToken{
			TaskType: ReduceTask,
			TaskID:   i,
		}
	}

	// 初始化 coordinator
	c := Coordinator{
		ControlLock:    sync.Mutex{},
		NMap:           nMap,
		NReduce:        nReduce,
		TaskList:       taskList,
		Stage:          MapStage,
		MapLock:        sync.Mutex{},
		MapNeed:        nMap,
		MapTaskChan:    mapTaskChan,
		ReduceLock:     sync.Mutex{},
		ReduceNeed:     nReduce,
		ReduceTaskChan: reduceTaskChan,
	}

	c.server()
	return &c
}

func (c *Coordinator) getTaskFromInfo(task *Task, taskInfo TaskInfo) {
	task.TaskID = taskInfo.TaskID
	task.TaskType = taskInfo.TaskType
	task.InputFiles = taskInfo.InputFiles
	task.NReduce = c.NReduce
}

func getMapTaskKey(taskID int) string {
	return fmt.Sprintf("map_%d", taskID)
}

func getReduceTaskKey(taskID int) string {
	return fmt.Sprintf("reduce_%d", taskID)
}

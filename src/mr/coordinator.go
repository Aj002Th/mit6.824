package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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
	// 这么加会宕机是为啥？
	//c.ControlLock.Lock()
	//defer c.ControlLock.Unlock()

	c.ControlLock.Lock()
	stage := c.Stage
	c.ControlLock.Unlock()

	switch stage {
	case MapStage:
		taskToken, ok := <-c.MapTaskChan
		log.Printf("[GetTask - map] TaskToken: %+v\n", taskToken)
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
			log.Printf("[GetTask - map] Task %v not found\n", taskToken.TaskID)
			reply.TaskType = WaitTask
			break
		}
		if taskInfo.TaskStatus == Done {
			// 走入这个分支的情景: 任务超时后重新被放入 chan, 还没下发原先执行该任务的 worker 就完成了任务
			// 忽略掉这个任务即可
			log.Printf("[GetTask - map] Task %v have been done\n", taskToken.TaskID)
			reply.TaskType = WaitTask
			break
		}
		taskInfo.TaskStatus = Working
		c.TaskList[taskKey] = taskInfo
		c.ControlLock.Unlock()
		// 组装 reply 信息
		c.getTaskFromInfo(reply, taskInfo)
		log.Printf("[GetTask - map] reply: %+v\n", reply)
		// 超时监控 - 超时重新分配任务
		go c.mapTaskTimeoutWatcher(taskToken)

	case ReduceStage:
		taskToken, ok := <-c.ReduceTaskChan
		log.Printf("[GetTask - reduce] TaskToken: %+v\n", taskToken)
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
		c.ControlLock.Unlock()
		// 组装 reply 信息
		c.getTaskFromInfo(reply, taskInfo)
		log.Printf("[GetTask - reduce] reply: %+v\n", reply)
		// 超时监控 - 超时重新分配任务
		go c.reduceTaskTimeoutWatcher(taskToken)

	case AllDoneStage:
		reply.TaskType = ExitTask
	}

	return nil
}

// map task timeout watcher
func (c *Coordinator) mapTaskTimeoutWatcher(taskToken TaskToken) {
	// 设置超时时间
	time.Sleep(Timeout)

	c.ControlLock.Lock()
	defer c.ControlLock.Unlock()

	// 检查任务是否完成
	taskKey := getMapTaskKey(taskToken.TaskID)
	taskInfo := c.TaskList[taskKey]
	if taskInfo.TaskStatus != Done {
		// 触发超时, 将任务重新放入 map 任务分配 chan
		c.MapTaskChan <- taskToken
		log.Printf("[mapTaskTimeoutWatcher] task timeout: %+v\n", taskToken)
	}
}

// reduce task timeout watcher
func (c *Coordinator) reduceTaskTimeoutWatcher(taskToken TaskToken) {
	// 设置超时时间
	time.Sleep(Timeout)

	c.ControlLock.Lock()
	defer c.ControlLock.Unlock()

	// 检查任务是否完成
	taskKey := getReduceTaskKey(taskToken.TaskID)
	taskInfo := c.TaskList[taskKey]
	if taskInfo.TaskStatus != Done {
		// 触发超时, 将任务重新放入 reduce 任务分配 chan
		c.ReduceTaskChan <- taskToken
		log.Printf("[reduceTaskTimeoutWatcher] task timeout: %+v\n", taskToken)
	}
}

func (c *Coordinator) FinishTask(args *Task, reply *Empty) error {
	log.Printf("[FinishTask] args: %+v\n", args)

	c.ControlLock.Lock()
	defer c.ControlLock.Unlock()

	switch args.TaskType {
	case MapStage:
		c.MapLock.Lock()
		defer c.MapLock.Unlock()
		// 生成真的的中间结果文件
		for i, tmpFile := range args.OutputFiles {
			fname := fmt.Sprintf("mr-%d-%d", args.TaskID, i)
			err := os.Rename(tmpFile, fname)
			if err != nil {
				log.Fatalf("[FinishTask - map stage] cannot rename file %v to %v: %v", tmpFile, fname, err)
			}
		}
		// 修改元信息
		taskKey := getMapTaskKey(args.TaskID)
		taskInfo := c.TaskList[taskKey]
		if taskInfo.TaskStatus != Done {
			// 修改任务状态
			taskInfo.TaskStatus = Done
			c.TaskList[taskKey] = taskInfo
			// 更新 MapNeed
			c.MapNeed--
			if c.MapNeed == 0 {
				c.Stage = ReduceStage
				log.Println("[FinishTask] change state to reduce stage")
			}
		}

	case ReduceStage:
		c.ReduceLock.Lock()
		defer c.ReduceLock.Unlock()
		// 生成真正的结果文件
		fname := fmt.Sprintf("mr-out-%d", args.TaskID)
		tmpFile := args.OutputFiles[0]
		err := os.Rename(tmpFile, fname)
		if err != nil {
			log.Fatalf("[FinishTask - reduce stage] cannot rename file %v to %v: %v", tmpFile, fname, err)
		}
		// 修改元信息
		taskKey := getReduceTaskKey(args.TaskID)
		taskInfo := c.TaskList[taskKey]
		if taskInfo.TaskStatus != Done {
			// 修改任务状态
			taskInfo.TaskStatus = Done
			c.TaskList[taskKey] = taskInfo
			// 更新 ReduceNeed
			c.ReduceNeed--
			if c.ReduceNeed == 0 {
				c.Stage = AllDoneStage
				log.Println("[FinishTask] change state to all done stage")
			}
		}

	default:
		log.Printf("[FinishTask] get a specific task type: :%v\n", args.TaskType)
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

	// 到达 AllDoneStage 说明任务已经结束
	c.ControlLock.Lock()
	defer c.ControlLock.Unlock()
	if c.Stage == AllDoneStage {
		ret = true
		log.Printf("[Done] ready to exit")
	}

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	coordinatorLogInit()

	log.Printf("[MakeCoordinator] get in function")
	nMap := len(files)
	taskList := make(map[string]TaskInfo, 0)

	// 创建 map 任务
	log.Printf("[MakeCoordinator] start create map task")
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
	log.Printf("[MakeCoordinator] start create reduce task")
	reduceTaskChan := make(chan TaskToken, nReduce)
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
	log.Printf("[MakeCoordinator] coordinator initialized")

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

func coordinatorLogInit() {
	log.SetPrefix("coordinator: ")
	if !Debug {
		log.SetOutput(ioutil.Discard)
	}
}

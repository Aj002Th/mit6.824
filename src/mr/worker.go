package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func workerLogInit() {
	log.SetPrefix("worker: ")
	if !Debug {
		log.SetOutput(ioutil.Discard)
	}
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerLogInit()

loop:
	for {
		empty, task := Empty{}, Task{}
		ok := call("Coordinator.GetTask", &empty, &task)
		if !ok {
			log.Println("[GetTask] failed: coordinator have exited")
			task.TaskType = ExitTask
		}
		log.Printf("[GetTask]  task: %+v\n", task)

		switch task.TaskType {
		case MapTask:
			log.Println("[map] task start")
			// 读取 task 对应文件
			log.Println("[map] read input file: start")
			filename := task.InputFiles[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Printf("[map] cannot open %v: %v\n", filename, err)
				break loop
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Printf("[map] cannot read %v: %v\n", filename, err)
				break loop
			}
			err = file.Close()
			if err != nil {
				log.Fatalf("[map] cannot close file %v: %v", filename, err)
			}
			log.Println("[map] read input file: end")
			// 完成 map 操作并将中间结果分组
			kva := mapf(filename, string(content))
			intermediate := make([]ByKey, task.NReduce)
			for _, kv := range kva {
				bucket := ihash(kv.Key) % task.NReduce
				intermediate[bucket] = append(intermediate[bucket], kv)
			}
			// 保存中间结果文件
			for i, bucket := range intermediate {
				// 使用临时文件,并在写入完成时更名,保证不会出现 worker 写一半崩溃,
				// 导致出现有有不完整的中间文件
				fname := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
				f, err := ioutil.TempFile("", fname)
				if err != nil {
					log.Fatalf("[map] cannot create temp file %v: %v", fname, err)
				}
				enc := json.NewEncoder(f)
				for _, kv := range bucket {
					err := enc.Encode(&kv)
					if err != nil {
						log.Printf("[map] cannot encode %v: %v", kv, err)
						break
					}
				}
				// 生成中间临时文件 - 写操作交给 master 来做
				task.OutputFiles = append(task.OutputFiles, f.Name())
				err = f.Close()
				if err != nil {
					log.Fatalf("[map] cannot close file %v: %v", f.Name(), err)
				}
			}
			// 汇报 master
			ok := call("Coordinator.FinishTask", &task, &empty)
			if !ok {
				log.Printf("[map] Finish task failed")
			}
			log.Println("[map] task finish")

		case ReduceTask:
			log.Println("[reduce] task start")
			// 同样使用临时文件来保证不会出现不完整的结果文件
			oname := fmt.Sprintf("mr-out-%d", task.TaskID)
			ofile, err := ioutil.TempFile("", oname)
			if err != nil {
				log.Fatalf("[reduce] cannot create temp file %v: %v", oname, err)
			}
			// 将多个中间文件一个个进行处理 -> mr-out文件中统计结果不统一
			// 需要将中间文件内容全部读入内存, 排序后统一处理
			kva := []KeyValue{}
			for _, fname := range task.InputFiles {
				file, err := os.Open(fname)
				if err != nil {
					log.Fatalf("[reduce] cannot open %v: %v", fname, err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						log.Printf("[reduce] cannot decode %v: %v", kv, err)
						break
					}
					kva = append(kva, kv)
				}
				err = file.Close()
				if err != nil {
					log.Fatalf("[reduce] cannot close file %v: %v", fname, err)
				}
			}
			// 将 key 相同的 value 合到一个数组里, 然后进行 reduce 操作
			sort.Sort(ByKey(kva))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				_, err = fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				if err != nil {
					log.Printf("[reduce] cannot write file: %v\n", err)
				}

				i = j
			}
			// 生成中间临时文件 - 写操作交给 master 来做
			task.OutputFiles = append(task.OutputFiles, ofile.Name())
			err = ofile.Close()
			if err != nil {
				log.Fatalf("[reduce] cannot close file %v: %v", ofile.Name(), err)
			}
			// 汇报 master
			ok := call("Coordinator.FinishTask", &task, &empty)
			if !ok {
				log.Printf("[reduce] Finish task failed")
			}
			log.Println("[reduce] task finish")

		case WaitTask:
			log.Println("[wait] task start")
			time.Sleep(WaitTaskDuration)

		case ExitTask:
			log.Println("[exit] receive exit command")
			break loop
		}
	}

	log.Println("worker exit")
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	time.Sleep(RpcDuration)

	if err == nil {
		return true
	}

	log.Printf("[call] %v failed: %v\n", rpcname, err)
	return false
}

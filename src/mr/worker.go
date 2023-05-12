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

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

loop:
	for {
		empty, task := Empty{}, Task{}
		ok := call("Coordinator.GetTask", &empty, &task)
		if !ok {
			task.TaskType = WaitTask
		}

		switch task.TaskType {
		case MapTask:
			log.Println("[map] task start")
			// 读取 task 对应文件
			filename := task.InputFiles[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("[map] cannot open %v: %v", filename, err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("[map] cannot read %v: %v", filename, err)
			}
			file.Close()
			// 完成 map 操作并将中间结果分组
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))
			intermediate := make([]ByKey, task.NReduce)
			for _, kv := range kva {
				bucket := ihash(kv.Key)
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
						// log.Fatalf("[map] cannot encode %v: %v", kv, err)
						break
					}
				}
				// 生成中间文件
				err = os.Rename(f.Name(), fname)
				if err != nil {
					log.Fatalf("[map] cannot rename file %v: %v", f.Name(), err)
				}
				task.OutputFiles = append(task.OutputFiles, fname)
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
			// 将多个中间文件一个个进行处理
			for _, fname := range task.InputFiles {
				kva := make([]KeyValue, 0)
				file, err := os.Open(fname)
				if err != nil {
					log.Fatalf("[map] cannot open %v: %v", fname, err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						// log.Fatalf("[reduce] cannot decode %v: %v", kv, err)
						break
					}
					kva = append(kva, kv)
				}
				// 将 key 相同的 value 合到一个数组里
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
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}
			}
			// 生成结果文件
			err = os.Rename(ofile.Name(), oname)
			if err != nil {
				log.Fatalf("[reduce] cannot rename %v: %v", ofile.Name(), err)
			}
			task.OutputFiles = append(task.OutputFiles, oname)
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
			time.Sleep(time.Second)

		case ExitTask:
			log.Panicln("exit")
			break loop
		}
	}

	log.Panicln("worker exit")
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
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

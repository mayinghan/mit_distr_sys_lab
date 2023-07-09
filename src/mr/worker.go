package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
)

const IntermediateDir = "./interresult"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func askForTask() (*GetJobResponse, bool) {
	req := GetJobRequest{}
	req.WorkerId = os.Getpid() // use pid since work will be run as a standalone process
	resp := GetJobResponse{}

	ok := call("Coordinator.GetJob", &req, &resp)
	if !ok {
		return nil, false
	}
	return &resp, true
}

func getReducerCount() (int, bool) {
	req := GetReducerCountRequest{}
	resp := GetReducerCountResponse{}

	ok := call("Coordinator.GetReducerCount", &req, &resp)
	if !ok {
		return 0, false
	}
	return resp.ReducerCount, true
}

// For each kv, write to corresponding reducer input file
// based on hash function
func writeMapResultToDisk(kv []KeyValue, mapperId int, nReducer int) {
	oPrefix := path.Join(IntermediateDir, string(mapperId))
	files := make([]*os.File, 0, nReducer)
	buffers := make([]*bufio.Writer, 0, nReducer)

	for i := 0; i < nReducer; i++ {
		output := fmt.Sprintf("%s-%d", oPrefix, i)
		file, _ := os.Create(output)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
	}

	for _, kv := range kv {
		idx := ihash(kv.Key) % nReducer
		encoder := json.NewEncoder(buffers[idx])
		encoder.Encode(&kv)
	}

	for _, buf := range buffers {
		buf.Flush()
	}

	for _, file := range files {
		file.Close()
	}
}

func writeReduceResultToDisk() {

}

func execMapper(mapf func(string, string) []KeyValue, inputPath string, jobId int, nReducer int) {
	raw_data, _ := ioutil.ReadFile(inputPath)

	kv := mapf(inputPath, string(raw_data))
	writeMapResultToDisk(kv, jobId, nReducer)
}

func execReducer(reducef func(string, []string) string, outputPath string, jobId int) {
	itmFilePattern := fmt.Sprintf("%s/%s-%d", IntermediateDir, "*", jobId)
	files, _ := filepath.Glob(itmFilePattern)

	reducedKV := make(map[string][]string)

	for _, file := range files {
		content, _ := os.ReadFile(file)
		var kv KeyValue
		if err := json.Unmarshal(content, &kv); err != nil {
			log.Printf("cannot unmarshell json content %s", string(content))
			return
		}
		reducedKV[kv.Key] = append(reducedKV[kv.Key], kv.Value)
	}

	writeReduceResultToDisk()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// loop asking for tasks from coordinator
	for {
		resp, ok := askForTask()
		nReducers, _ := getReducerCount()
		log.Printf("number of reducers %d", nReducers)
		if !ok {
			log.Printf("work %d start failed", os.Getpid())
			return
		}
		if resp.Status == AllDone {
			log.Println("no more tasks available, exit worker")
			return
		}
		if resp.Status == NoPending {
			continue
		}

		if resp.JobType == "mapper" {
			execMapper(mapf, resp.Input, resp.JobId, nReducers)
		} else if resp.JobType == "reducer" {
			execReducer(reducef, resp.Output, resp.JobId)
		}

	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

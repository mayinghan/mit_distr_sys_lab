package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"sync"
)

const IntermediateDir = "./interresult"
const OutputDir = "./mr_output"

func handleHandler() {
	if err := recover(); err != nil {
		log.Println("recovered")
	}
}

type JobMeta struct {
	Id             int
	Type           string
	Input          string
	Output         string
	State          int // 0: initing 1: running 2: succeeded 4: failed 5: killed
	AssignedWorkId int
}

type Coordinator struct {
	// Your definitions here.
	mutex    sync.Mutex
	nMapper  int
	nReducer int
	mappers  []JobMeta
	reducers []JobMeta
}

func (c *Coordinator) GetJob(req *GetJobRequest, resp *GetJobResponse) {
	defer handleHandler()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var job *JobMeta
	if c.nMapper > 0 {
		job = c.fetchTask(c.mappers)
		if job == nil {
			log.Printf("No more available mapper tasks")
			resp.Status = -1
			return
		}
	} else if c.nReducer > 0 {
		job = c.fetchTask(c.reducers)
		if job == nil {
			log.Printf("No more available reducer tasks")
			resp.Status = -1
			return
		}
	} else {
		resp.Status = 100
		return
	}

	job.AssignedWorkId = req.WorkerId
	job.State = 1

	resp.Input = job.Input
	resp.Output = job.Output
	resp.JobType = job.Type
	resp.JobId = job.Id

	return
}

func (c *Coordinator) fetchTask(jobList []JobMeta) (job *JobMeta) {
	for i := 0; i < len(jobList); i++ {
		if jobList[i].State == 0 && jobList[i].AssignedWorkId == -1 {
			job = &c.mappers[i]
			return
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.nMapper == 0 && c.nReducer == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	nMap := len(files)
	c.mappers = make([]JobMeta, 0, nMap)
	c.reducers = make([]JobMeta, 0, nReduce)
	c.nMapper = nMap
	c.nReducer = nReduce
	for i := 0; i < nMap; i++ {
		interResultOutput := path.Join(IntermediateDir, "map", strconv.Itoa(i))
		c.mappers = append(c.mappers, JobMeta{i, "mapper", files[i], interResultOutput, 0, -1})
	}

	for i := 0; i < nReduce; i++ {
		// leave input path empty for now since there will be shuffle based on reduce logic
		mrOutput := path.Join(OutputDir, "mr-out-"+strconv.Itoa(i))
		c.reducers = append(c.reducers, JobMeta{i, "reducer", "", mrOutput, 0, -1})
	}
	c.server()

	// TODO: clean up intermediate result
	// os.RemoveAll(IntermediateDir)

	return &c
}

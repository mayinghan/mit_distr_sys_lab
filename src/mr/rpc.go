package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type GetJobRequest struct {
	WorkerId int
}

type GetJobStatus int32

const (
	Success GetJobStatus = iota
	Fail
	NoPending
	AllDone
)

type GetJobResponse struct {
	Status  GetJobStatus
	JobType string
	JobId   int
	Input   string
	Output  string
}

type GetReducerCountRequest struct {
}

type GetReducerCountResponse struct {
	ReducerCount int
}

type ReportJobRequest struct {
	WorkerId int
	JobType  string
	JobId    int
	Status   int
}

type ReportJobResponse struct {
	Status int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	JobChanMap    chan *Job
	JobChanReduce chan *Job
	ReducerNum    int
	MapNum        int
	uniqueJobId   int
	Phase         Phase
	jobMetaHolder JobMetaHolder
}
type Job struct {
	JobType    JobType
	InputFile  []string
	JobId      int
	ReducerNum int
}
type JobMetaInfo struct {
	condition JobCondition
	job       *Job
	startTime time.Time
}
type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo
}

var mu sync.Mutex

//往任务管理器中添加任务
func (j *JobMetaHolder) putJob(JobInfo *JobMetaInfo) bool {
	jobId := JobInfo.job.JobId
	meta, _ := j.MetaMap[jobId]
	if meta != nil {
		fmt.Println("meta contains job which id = ", jobId)
		return false
	} else {
		j.MetaMap[jobId] = JobInfo
	}
	return true
}

//当任务被派发出去时，更改信息以便后续处理
func (j *JobMetaHolder) fireTheJob(jobId int) bool {
	meta, ok := j.MetaMap[jobId]
	if !ok || meta.condition != JobWaiting {
		fmt.Println("job distributes wrong")
		return false
	}
	meta.condition = JobWorking
	meta.startTime = time.Now()
	return true
}
func (c *Coordinator) checkJobDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range c.jobMetaHolder.MetaMap {
		if v.job.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum += 1
			} else {
				mapUndoneNum++
			}
		} else {
			if v.condition == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	return (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0)
}

func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(2 * time.Second)
		mu.Lock()
		if c.Phase == AllDone {
			mu.Unlock()
			os.Exit(1)
		}
		now := time.Now()
		for _, v := range c.jobMetaHolder.MetaMap {
			if v.job.JobType == MapJob {
				if v.condition == JobWorking && v.startTime.Add(time.Second*5).Before(now) {
					v.condition = JobWaiting
					c.JobChanMap <- v.job
					fmt.Println("occur a crash on map job ", v.job.JobId)
				}
			} else if v.job.JobType == ReduceJob {
				if v.condition == JobWorking && v.startTime.Add(time.Second*5).Before(now) {
					fmt.Println("occur a crash on reduce job ", v.job.JobId)
					v.condition = JobWaiting
					c.JobChanReduce <- v.job
				}
			}
		}
		mu.Unlock()
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) generateJobId() int {
	res := c.uniqueJobId
	c.uniqueJobId++
	return res
}

//生成job
func (c *Coordinator) makeMapJobs(files []string) {
	for _, v := range files {
		id := c.generateJobId()
		job := Job{
			JobType:    MapJob,
			InputFile:  []string{v},
			JobId:      id,
			ReducerNum: c.ReducerNum,
		}
		jobMetaInfo := JobMetaInfo{
			condition: JobWaiting,
			job:       &job,
		}
		c.jobMetaHolder.putJob(&jobMetaInfo)
		fmt.Println("making map job:", &job)
		c.JobChanMap <- &job
	}
	fmt.Println("done making map jobs")
	c.checkJobDone()
}

func (c *Coordinator) makeReduceJobs() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		fmt.Println("making reduce job :", id)
		JobToDo := Job{
			JobType:   ReduceJob,
			JobId:     id,
			InputFile: TmpFileAssignHelper(i, "./"),
		}
		jobMetaInfo := JobMetaInfo{
			condition: JobWaiting,
			job:       &JobToDo,
		}
		c.jobMetaHolder.putJob(&jobMetaInfo)
		c.JobChanReduce <- &JobToDo
	}
	//defer close(c.JobChannelReduce)
	fmt.Println("done making reduce jobs")
	c.checkJobDone()
}

func TmpFileAssignHelper(whichReduce int, wd string) []string {
	var res []string
	rd, _ := ioutil.ReadDir(wd)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}

func (c *Coordinator) nextPhase() {
	if c.Phase == MapPhase {
		c.makeReduceJobs()
		c.Phase = ReducePhase
	} else if c.Phase == ReducePhase {
		c.Phase = AllDone
	}
}

func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("coordinator get a request from worker:")
	if c.Phase == MapPhase {
		if len(c.JobChanMap) > 0 {
			*reply = *<-c.JobChanMap
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				fmt.Println("fail to run map job: ", reply.JobId)
			}
		} else {
			reply.JobType = WaittingJob
			if c.checkJobDone() {
				c.nextPhase()
			}
			return nil
		}
	} else if c.Phase == ReducePhase {
		if len(c.JobChanReduce) > 0 {
			*reply = *<-c.JobChanReduce
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				fmt.Println("fail to run reduce job: ", reply.JobId)
			}
		} else {
			reply.JobType = WaittingJob
			if c.checkJobDone() {
				c.nextPhase()
			}
			return nil
		}
	} else {
		reply.JobType = KillJob
	}
	return nil
}

func (c *Coordinator) JobIsDone(args *Job, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.JobType {
	case MapJob:
		job, ok := c.jobMetaHolder.MetaMap[args.JobId]
		if ok && job.condition == JobWorking {
			job.condition = JobDone
			fmt.Println("done map job:", args.JobId)
		} else {
			fmt.Println("wrong when done map job:", args.JobId)
		}
	case ReduceJob:
		job, ok := c.jobMetaHolder.MetaMap[args.JobId]
		if ok && job.condition == JobWorking {
			job.condition = JobDone
			fmt.Println("done reduce job:", args.JobId)
		} else {
			fmt.Println("wrong when done reduce job:", args.JobId)
		}
	default:
		panic("wrong done job")
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	fmt.Println("++++++++++++query for if Done++++++++++++")
	return c.Phase == AllDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChanMap:    make(chan *Job, len(files)),
		JobChanReduce: make(chan *Job, nReduce),
		jobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
		Phase:       MapPhase,
		ReducerNum:  nReduce,
		MapNum:      len(files),
		uniqueJobId: 0,
	}
	c.makeMapJobs(files)
	c.server()
	go c.CrashHandler()
	return &c
}

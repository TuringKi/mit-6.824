package mapreduce

import "container/list"
import "fmt"

import "sync"
import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	state int
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	log.Printf("RunMaster start...\n")
	go func() {
		for {
			j := <-mr.registerChannel
			mr.mu.Lock()
			mr.Workers[j] = &WorkerInfo{j, 0}
			mr.mu.Unlock()
			fmt.Printf("Worker %v registered\n", j)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < mr.nMap; i++ {
		var worker string
		for {
			mr.mu.Lock()
			for _, value := range mr.Workers {
				if value.state == 0 {
					worker = value.address
					value.state = 1
					break
				}
			}
			mr.mu.Unlock()
			if worker != "" {
				break
			}
		}
		log.Printf("Map to worker %v\n", worker)
		job_args := &DoJobArgs{mr.file, JobType(Map), i, mr.nReduce}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var job_reply DoJobReply
			ok := call(worker, "Worker.DoJob", job_args, &job_reply)
			for ok == false || job_reply.OK == false {
				log.Printf("Worker %v DoJob Error!\n", worker)
				mr.Workers[worker].state = -1
				log.Printf("Find a new Worker to DoJob!\n")
				worker = ""
				for {
					mr.mu.Lock()
					for _, value := range mr.Workers {
						if value.state == 0 {
							worker = value.address
							value.state = 1
							break
						}
					}
					mr.mu.Unlock()
					if worker != "" {
						break
					}
				}
				ok = call(worker, "Worker.DoJob", job_args, &job_reply)
			}
			mr.Workers[worker].state = 0
		}()
	}
	wg.Wait()
	for i := 0; i < mr.nReduce; i++ {
		var worker string
		for {
			mr.mu.Lock()
			for _, value := range mr.Workers {
				if value.state == 0 {
					worker = value.address
					value.state = 1
					break
				}
			}
			mr.mu.Unlock()
			if worker != "" {
				break
			}
		}
		job_args := &DoJobArgs{mr.file, JobType(Reduce), i, mr.nMap}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var job_reply DoJobReply
			ok := call(worker, "Worker.DoJob", job_args, &job_reply)
			if ok == false || job_reply.OK == false {
				log.Printf("Worker DoJob Error!\n")
				mr.Workers[worker].state = -1
				worker = ""
				for {
					mr.mu.Lock()
					for _, value := range mr.Workers {
						if value.state == 0 {
							worker = value.address
							value.state = 1
							break
						}
					}
					mr.mu.Unlock()
					if worker != "" {
						break
					}
				}
				ok = call(worker, "Worker.DoJob", job_args, &job_reply)
			}
			mr.Workers[worker].state = 0
		}()
	}
	wg.Wait()

	return mr.KillWorkers()
}

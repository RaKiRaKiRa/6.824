package mapreduce

import (
	"fmt"
	"log"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//	the mapFiles argument holds the names of the files that are the inputs to the map phase
//  nReduce is the number of reduce tasks.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int	// number
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var waitGroup sync.WaitGroup;

	for i := 0; i < ntasks; i++ {
		waitGroup.Add(i)

		var taskArgs DoTaskArgs
		taskArgs.JobName = jobName
		taskArgs.TaskNumber = i
		if(phase == mapPhase){
			taskArgs.File = mapFiles[i]
		}
		taskArgs.Phase = phase
		taskArgs.NumOtherPhase = n_other
		go func(){
			defer waitGroup.Done(); // do it when func end
			worker := <-registerChan
			if(call(worker, "Worker.DoTask", &taskArgs, nil) != true){
				log.Fatal("RPC call err")
			}
			go func(){registerChan <- worker}()
		}()
	}
	waitGroup.Wait() // wait
	fmt.Printf("Schedule: %v done\n", phase)
}

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	scheduling := NewScheduler(2, tasks, 2*time.Second)
	scheduling.Loop(ctx)
}

// Scheduler executes tasks with the following constraint:
// - Tasks are refreshed from taskFn every 'refreshDuration'
// - No tasks inanition
// - Same task can't execute more than once at a time.
// - 'concurrency' tasks executing at a time.
// - At least 500msec between execution for the same task.
type Scheduler struct {
	tasks           map[int]Task
	refreshFn       func() []int
	refreshDuration time.Duration
	runningTasks    chan int
}

type Task struct {
	ID            int
	lastExecution time.Time
}

func NewScheduler(concurrency int, refreshFn func() []int, refreshDuration time.Duration) *Scheduler {
	return &Scheduler{
		tasks:           make(map[int]Task),
		refreshFn:       refreshFn,
		refreshDuration: refreshDuration,
		runningTasks:    make(chan int, concurrency),
	}
}

func (s *Scheduler) Loop(ctx context.Context) {
	ticker := time.NewTicker(s.refreshDuration)
	wg := sync.WaitGroup{}
	sync := sync.Mutex{}
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			go func() {
				for _, v := range s.refreshFn() {
					fmt.Println("new task", v)
					sync.Lock()
					s.tasks[v] = Task{ID: v}
					sync.Unlock()
					s.runningTasks <- v
					fmt.Println("task", v, "added to running tasks")
				}
			}()
		case <-ctx.Done():
			wg.Wait()
			return
		case runningTask := <-s.runningTasks:
			if !s.tasks[runningTask].lastExecution.IsZero() && s.tasks[runningTask].lastExecution.Add(500*time.Millisecond).After(time.Now()) {
				fmt.Println("task", runningTask, "not ready to execute")
				continue
			}
			task := s.tasks[runningTask]
			task.lastExecution = time.Now()
			sync.Lock()
			s.tasks[runningTask] = task
			sync.Unlock()
			go func(task int) {
				wg.Add(1)
				defer wg.Done()
				execute(task)
			}(runningTask)
		}
	}
}

func tasks() []int {
	numberOfTasks := rand.Intn(5)
	if numberOfTasks == 0 {
		return []int{}
	}

	tasks := make([]int, 0, numberOfTasks)
	for i := 0; i < numberOfTasks; i++ {
		tasks = append(tasks, i)
	}

	return tasks
}

func execute(task int) {
	fmt.Println("executed", task)
}

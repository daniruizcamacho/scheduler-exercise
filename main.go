package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
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
	tasks           []int
	concurrency     int
	refreshFn       func() []int
	refreshDuration time.Duration
}

func NewScheduler(concurrency int, refreshFn func() []int, refreshDuration time.Duration) *Scheduler {
	return &Scheduler{
		concurrency:     concurrency,
		refreshFn:       refreshFn,
		refreshDuration: refreshDuration,
		tasks:           []int{},
	}
}

func (s *Scheduler) Loop(ctx context.Context) {
	ticker := time.NewTicker(s.refreshDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.tasks = append(s.tasks, s.refreshFn()...)
		case <-ctx.Done():
			return
		default:
			fmt.Println(s.tasks)
			time.Sleep(1 * time.Second)
			if len(s.tasks) == 0 {
				continue
			}

			numberOfTasksToExecute := s.concurrency
			if len(s.tasks) < s.concurrency {
				numberOfTasksToExecute = len(s.tasks)
			}

			tasksToExecute := s.tasks[:numberOfTasksToExecute]
			s.tasks = s.tasks[numberOfTasksToExecute:]
			for _, task := range tasksToExecute {
				go execute(task)
			}
		}
	}
}

func tasks() []int {
	numberOfTasks := rand.Intn(5)
	if numberOfTasks == 0 {
		return []int{}
	}

	fmt.Println(numberOfTasks)

	tasks := make([]int, 0, numberOfTasks)
	for i := 0; i < numberOfTasks; i++ {
		tasks = append(tasks, i)
	}

	return tasks
}

func execute(task int) {
	fmt.Println("executed", task)
}

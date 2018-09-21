// Package scheduler is a small library that you can use within your application that enables you to execute callbacks (goroutines) after a pre-defined amount of time. GTS also provides task storage which is used to invoke callbacks for tasks which couldn’t be executed during down-time as well as maintaining a history of the callbacks that got executed.
package scheduler

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
	"time"

	"github.com/sdvallejo/scheduler/storage"
	"github.com/sdvallejo/scheduler/task"
)

// Scheduler is used to schedule tasks. It holds information about those tasks
// including metadata such as argument types and schedule times
type Scheduler struct {
	funcRegistry *task.FuncRegistry
	stopChan     chan bool
	tasks        map[task.ID]*task.Task
	taskStore    storeBridge
}

// New will return a new instance of the Scheduler struct.
func New(store storage.TaskStore) Scheduler {
	funcRegistry := task.NewFuncRegistry()
	return Scheduler{
		funcRegistry: funcRegistry,
		stopChan:     make(chan bool),
		tasks:        make(map[task.ID]*task.Task),
		taskStore: storeBridge{
			store:        store,
			funcRegistry: funcRegistry,
		},
	}
}

func (scheduler *Scheduler) RegisterFunction(function task.Function) error {
	_, err := scheduler.funcRegistry.Add(function)
	return err
}

// RunAt will schedule function to be executed once at the given time.
func (scheduler *Scheduler) RunAt(time time.Time, function task.Function, params ...task.Param) (task.ID, error) {
	funcMeta, err := scheduler.funcRegistry.Add(function)
	if err != nil {
		return "", err
	}

	task := task.New(funcMeta, params)

	task.NextRun = time

	scheduler.registerTask(task)
	return task.Hash(), nil
}

// RunAfter executes function once after a specific duration has elapsed.
func (scheduler *Scheduler) RunAfter(duration time.Duration, function task.Function, params ...task.Param) (task.ID, error) {
	return scheduler.RunAt(time.Now().Add(duration), function, params...)
}

// RunEvery will schedule function to be executed every time the duration has elapsed.
func (scheduler *Scheduler) RunEvery(duration time.Duration, function task.Function, params ...task.Param) (task.ID, error) {
	t, exists := scheduler.taskExists(function)

	if exists && t.Duration == duration {
		return t.Hash(), nil
	}

	funcMeta, err := scheduler.funcRegistry.Add(function)
	if err != nil {
		return "", err
	}

	task := task.New(funcMeta, params)

	task.IsRecurring = true
	task.Duration = duration

	if exists {
		task.NextRun = t.LastRun.Add(duration)
		scheduler.removeTask(t)
	} else {
		task.NextRun = time.Now().Add(duration)
	}

	scheduler.registerTask(task)
	return task.Hash(), nil
}

// Start will run the scheduler's timer and will trigger the execution
// of tasks depending on their schedule.
func (scheduler *Scheduler) Start() error {
	// Populate tasks from storage
	if err := scheduler.populateTasks(); err != nil {
		return err
	}
	if err := scheduler.persistRegisteredTasks(); err != nil {
		return err
	}
	scheduler.runPending()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				scheduler.runPending()
			case <-scheduler.stopChan:
				close(scheduler.stopChan)
				return
			}
		}
	}()

	return nil
}

// Stop will put the scheduler to halt
func (scheduler *Scheduler) Stop() {
	scheduler.stopChan <- true
}

// Wait is a convenience function for blocking until the scheduler is stopped.
func (scheduler *Scheduler) Wait() {
	<-scheduler.stopChan
}

// Cancel is used to cancel the planned execution of a specific task using it's ID.
// The ID is returned when the task was scheduled using RunAt, RunAfter or RunEvery
func (scheduler *Scheduler) Cancel(taskID task.ID) error {
	task, found := scheduler.tasks[taskID]
	if !found {
		return fmt.Errorf("Task not found")
	}

	_ = scheduler.taskStore.Remove(task)
	delete(scheduler.tasks, taskID)
	return nil
}

// Clear will cancel the execution and clear all registered tasks.
func (scheduler *Scheduler) Clear() {
	for taskID, currentTask := range scheduler.tasks {
		_ = scheduler.taskStore.Remove(currentTask)
		delete(scheduler.tasks, taskID)
	}
	scheduler.funcRegistry = task.NewFuncRegistry()
}

func (scheduler *Scheduler) populateTasks() error {
	tasks, err := scheduler.taskStore.Fetch()
	if err != nil {
		return err
	}

	for _, dbTask := range tasks {
		// If we can't find the function, it's been changed/removed by user
		exists := scheduler.funcRegistry.Exists(dbTask.Func.Name)
		if !exists {
			log.Printf("%s was not found, it will be removed\n", dbTask.Func.Name)
			_ = scheduler.taskStore.Remove(dbTask)
			continue
		}

		// If the task instance is still registered with the same computed hash then move on.
		// Otherwise, one of the attributes changed and therefore, the task instance should
		// be added to the list of tasks to be executed with the stored params
		registeredTask, ok := scheduler.tasks[dbTask.Hash()]
		if !ok {
			log.Printf("Detected a change in attributes of one of the instances of task %s, \n",
				dbTask.Func.Name)
			dbTask.Func, _ = scheduler.funcRegistry.Get(dbTask.Func.Name)
			registeredTask = dbTask
			scheduler.tasks[dbTask.Hash()] = registeredTask
		}

		// Duration may have changed for recurring tasks
		if dbTask.IsRecurring && registeredTask.Duration != dbTask.Duration {
			// Reschedule NextRun based on dbTask.LastRun + registeredTask.Duration
			registeredTask.NextRun = dbTask.LastRun.Add(registeredTask.Duration)
		}
	}
	return nil
}

func (scheduler *Scheduler) persistRegisteredTasks() error {
	for _, task := range scheduler.tasks {
		err := scheduler.taskStore.Add(task)
		if err != nil {
			return err
		}
	}
	return nil
}

func (scheduler *Scheduler) runPending() {
	for _, task := range scheduler.tasks {
		if task.IsDue() {
			go scheduler.runTask(task)
		}
	}
}

func (scheduler *Scheduler) runTask(task *task.Task) {
	task.Run()

	if !task.IsRecurring {
		scheduler.removeTask(task)
	}

	scheduler.persistRegisteredTasks()
}

func (scheduler *Scheduler) removeTask(task *task.Task) {
	_ = scheduler.taskStore.Remove(task)
	delete(scheduler.tasks, task.Hash())
	scheduler.persistRegisteredTasks()
}

func (scheduler *Scheduler) registerTask(task *task.Task) {
	_, _ = scheduler.funcRegistry.Add(task.Func)
	scheduler.tasks[task.Hash()] = task
	scheduler.persistRegisteredTasks()
}

func (scheduler *Scheduler) taskExists(function task.Function) (*task.Task, bool) {
	funcValue := reflect.ValueOf(function)
	name := runtime.FuncForPC(funcValue.Pointer()).Name()
	for _, t := range scheduler.tasks {
		if t.Func.Name == name {
			return t, true
		}
	}
	return nil, false
}

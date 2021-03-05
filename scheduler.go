package gotask

import (
	"fmt"
	"strconv"
	"time"
)

var workerID = 0

type TaskFunc func(args ...interface{})
type EventFunc func(context interface{}, args ...interface{})

type Task struct {
	ID   uint64
	Func TaskFunc
	Args []interface{}
}

type Event struct {
	ID    uint64
	Event string
	Func  EventFunc
	Args  []interface{}
}

type TaskResult struct {
	ID    uint64
	Error error
}

type Worker struct {
	ID         string
	Context    interface{}
	TaskQueue  chan Task
	EventQueue chan Event
	NotifyCh   chan interface{}
}

func NewWorker(context interface{}, notify chan interface{}) (*Worker, error) {
	worker := &Worker{
		ID:         "worker_" + strconv.Itoa(workerID),
		Context:    context,
		TaskQueue:  make(chan Task, 1024),
		EventQueue: make(chan Event, 1024),
		NotifyCh:   notify,
	}
	workerID++

	return worker, nil
}

func (w *Worker) Start() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case task := <-w.TaskQueue:
			if task.Args != nil {
				task.Func(task.Args...)
			} else {
				task.Func()
			}
		case event := <-w.EventQueue:
			if event.Args != nil {
				event.Func(w.Context, event.Args...)
			} else {
				event.Func(w.Context)
			}
		case <-ticker.C:
			//TODO: do some report or cleanup
		case <-w.NotifyCh:
			//TODO: check notify event
			break
		}
	}
}

type TaskScheduler struct {
	Workers []*Worker
	Events  map[string]EventFunc
}

func NewTaskScheduler() (*TaskScheduler, error) {
	return &TaskScheduler{
		Workers: make([]*Worker, 0),
		Events:  make(map[string]EventFunc),
	}, nil
}

func (p *TaskScheduler) AddWorker(context interface{}, notify chan interface{}) error {
	worker, err := NewWorker(context, notify)

	if err != nil {
		return err
	}
	p.Workers = append(p.Workers, worker)
	go worker.Start()

	return nil
}

func (p *TaskScheduler) RegisterEvent(name string, fn EventFunc) error {
	//TODO: check if name is already registered?
	p.Events[name] = fn
	return nil
}

func (p *TaskScheduler) scheduleTask(timeout <-chan time.Time, id uint64, fn TaskFunc, args ...interface{}) error {
	t := Task{
		ID:   id,
		Func: fn,
		Args: args,
	}

	if len(p.Workers) == 0 {
		return ErrNoWorker
	}

	//task id is used to keep 'sticky' tasks,
	//i.e same task id goes to the same worker
	idx := id % uint64(len(p.Workers))
	select {
	case <-timeout:
		return ErrScheduleTimeout
	case p.Workers[idx].TaskQueue <- t:
		return nil
	}

	return nil
}

func (p *TaskScheduler) PostTask(id uint64, fn TaskFunc, args ...interface{}) error {
	return p.scheduleTask(nil, id, fn, args...)
}

func (p *TaskScheduler) scheduleEvent(timeout <-chan time.Time, id uint64,
	event string, fn EventFunc, args ...interface{}) error {

	e := Event{
		ID:    id,
		Event: event,
		Func:  fn,
		Args:  args,
	}

	if len(p.Workers) == 0 {
		return ErrNoWorker
	}

	//task id is used to keep 'sticky' tasks,
	//i.e same task id goes to the same worker
	idx := id % uint64(len(p.Workers))
	select {
	case <-timeout:
		return ErrScheduleTimeout
	case p.Workers[idx].EventQueue <- e:
		return nil
	}

	return nil
}

func (p *TaskScheduler) PostEvent(id uint64, event string, args ...interface{}) error {
	if fn, ok := p.Events[event]; ok {
		return p.scheduleEvent(nil, id, event, fn, args...)
	} else {
		return fmt.Errorf("Event not registered, event=%v", event)
	}
	return nil
}

func (p *TaskScheduler) PostEventTimeout(timeout time.Duration, id uint64, event string, args ...interface{}) error {
	if fn, ok := p.Events[event]; ok {
		return p.scheduleEvent(time.After(timeout), id, event, fn, args...)
	} else {
		return fmt.Errorf("Event not registered, event=%v", event)
	}
	return nil
}

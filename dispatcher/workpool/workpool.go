package workpool

import (
	"context"
	"errors"
	"sync"

	"github.com/kordar/goetl"
	logger "github.com/kordar/gologger"
)

type TaskID string

type HandlerFunc func(ctx context.Context, msg goetl.Message) error

type TaskHandle struct {
	Name             string
	Container        map[TaskID]HandlerFunc
	WorkerPoolSize   int
	TaskQueueBuffLen int
	TaskQueue        []chan workItem

	mu      sync.RWMutex
	msgId   int
	started bool
	wg      sync.WaitGroup
}

var ErrTaskQueueFull = errors.New("task queue full")

type workItem struct {
	taskID TaskID
	msg    goetl.Message
}

func NewTaskHandle(workSize int, queueBuffLen int) *TaskHandle {
	return NewTaskHandleWithName("gotask", workSize, queueBuffLen)
}

func NewTaskHandleWithName(name string, workSize int, queueBuffLen int) *TaskHandle {
	if workSize <= 0 {
		workSize = 1
	}
	if queueBuffLen <= 0 {
		queueBuffLen = 1024
	}
	return &TaskHandle{
		Container:        make(map[TaskID]HandlerFunc),
		WorkerPoolSize:   workSize,
		TaskQueueBuffLen: queueBuffLen,
		Name:             name,
		TaskQueue:        make([]chan workItem, workSize),
	}
}

func (mh *TaskHandle) AddTask(taskID string, handler HandlerFunc) {
	if taskID == "" || handler == nil {
		return
	}
	id := TaskID(taskID)
	mh.mu.Lock()
	defer mh.mu.Unlock()
	if _, ok := mh.Container[id]; ok {
		panic("repeated func , taskId = " + taskID)
	}
	mh.Container[id] = handler
	logger.Infof("[%s] the task named '%s' was added successfully.", mh.Name, taskID)
}

func (mh *TaskHandle) StartWorkerPool(ctx context.Context, errChan chan<- error) {
	mh.mu.Lock()
	if mh.started {
		mh.mu.Unlock()
		return
	}
	mh.started = true
	mh.mu.Unlock()

	for i := 0; i < mh.WorkerPoolSize; i++ {
		mh.TaskQueue[i] = make(chan workItem, mh.TaskQueueBuffLen)
		mh.wg.Add(1)
		go mh.startOneWorker(ctx, errChan, i, mh.TaskQueue[i])
	}
}

func (mh *TaskHandle) Wait() {
	mh.wg.Wait()
}

func (mh *TaskHandle) SendToTaskQueue(ctx context.Context, errChan chan<- error, taskID string, msg goetl.Message) {
	workerID := mh.nextWorkerID()
	mh.sendToWorker(ctx, errChan, workItem{taskID: TaskID(taskID), msg: msg}, workerID)
}

func (mh *TaskHandle) SendToTaskQueueP(ctx context.Context, errChan chan<- error, taskID string, msg goetl.Message, pools []int) {
	if len(pools) == 0 {
		mh.SendToTaskQueue(ctx, errChan, taskID, msg)
		return
	}
	index := mh.nextMsgId() % len(pools)
	workerID := pools[index]
	mh.sendToWorker(ctx, errChan, workItem{taskID: TaskID(taskID), msg: msg}, workerID)
}

func (mh *TaskHandle) SendToTaskQueueN(ctx context.Context, errChan chan<- error, taskID string, msg goetl.Message, workerID int) {
	mh.sendToWorker(ctx, errChan, workItem{taskID: TaskID(taskID), msg: msg}, workerID)
	mh.nextMsgId()
}

func (mh *TaskHandle) nextWorkerID() int {
	return mh.nextMsgId() % mh.WorkerPoolSize
}

func (mh *TaskHandle) nextMsgId() int {
	mh.mu.Lock()
	defer mh.mu.Unlock()
	if mh.msgId > 1000000 {
		mh.msgId = 0
	} else {
		mh.msgId++
	}
	return mh.msgId
}

func (mh *TaskHandle) sendToWorker(ctx context.Context, errChan chan<- error, item workItem, workerID int) {
	if workerID < 0 || workerID >= mh.WorkerPoolSize {
		return
	}
	q := mh.TaskQueue[workerID]
	select {
	case <-ctx.Done():
		return
	case q <- item:
	default:
		select {
		case errChan <- ErrTaskQueueFull:
		default:
		}
	}
}

func (mh *TaskHandle) doMsgHandler(ctx context.Context, errChan chan<- error, item workItem) {
	mh.mu.RLock()
	handler, ok := mh.Container[item.taskID]
	mh.mu.RUnlock()
	if !ok {
		logger.Infof("[%s] no task named '%s' was found..", mh.Name, string(item.taskID))
		return
	}
	if err := handler(ctx, item.msg); err != nil {
		select {
		case errChan <- err:
		default:
		}
	}
}

func (mh *TaskHandle) startOneWorker(ctx context.Context, errChan chan<- error, workerID int, taskQueue chan workItem) {
	defer mh.wg.Done()
	logger.Infof("[%s] WorkerID = %d is starting.", mh.Name, workerID)
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-taskQueue:
			mh.doMsgHandler(ctx, errChan, item)
		}
	}
}

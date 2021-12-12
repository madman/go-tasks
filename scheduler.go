package tasks

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

const (
	missedTaskTimeout = 10 * time.Second
	throttleDelay     = 1000 * time.Millisecond
	maxTimeWindow     = 5 * time.Second
)

var DefaultCurrentTimeResolver = func() time.Time { return time.Now().UTC() }

type (
	BaseScheduler interface {
		AddTask(ctx context.Context, task *Task, at time.Time) error
		ProcessTasks(ctx context.Context, callback Callback) error
		RemoveTask(ctx context.Context, id string) error
	}

	CurrentTimeResolver func() time.Time

	Callback func(ctx context.Context, task *Task) error

	Task struct {
		ID      string
		Time    time.Time
		Delay   time.Duration // if delay > 0 new task time will be based on current time + delay.
		Payload []byte
	}

	Scheduler struct {
		threads        int
		storage        Storage
		getCurrentTime CurrentTimeResolver
		wg             *sync.WaitGroup
		ch             chan *Task
		done           *atomic.Bool
		logger         *log.Logger
	}
)

func NewScheduler(
	threads int,
	storage Storage,
	getCurrentTime CurrentTimeResolver,
	logger *log.Logger,
) BaseScheduler {
	return &Scheduler{
		threads:        threads,
		storage:        storage,
		getCurrentTime: getCurrentTime,
		wg:             &sync.WaitGroup{},
		ch:             make(chan *Task, threads*2),
		done:           atomic.NewBool(false),
		logger:         logger,
	}
}

func (rcv *Scheduler) AddTask(ctx context.Context, task *Task, at time.Time) error {
	return rcv.storage.Add(ctx, task, at)
}

func (rcv *Scheduler) RemoveTask(ctx context.Context, id string) error {
	return rcv.storage.Remove(ctx, id)
}

func (rcv *Scheduler) ProcessTasks(ctx context.Context, callback Callback) error {
	go rcv.runMissedTasksRestorer(ctx)

	for i := 0; i < rcv.threads; i++ {
		w := NewWorker(callback, rcv.storage, rcv.getCurrentTime, rcv.logger)
		rcv.wg.Add(1)
		go func() {
			defer rcv.wg.Done()
			w.Do(ctx, rcv.ch)
		}()
	}

	shouldStop := false
	done := make(chan bool)
	go func() {
		shouldThrottle := false

		for !shouldStop {
			if shouldThrottle {
				time.Sleep(throttleDelay)
			}

			task, err := rcv.storage.GetNext(ctx)
			if err != nil {
				if errors.Is(err, ErrNoTasks) {
					shouldThrottle = true
				} else {
					rcv.logger.Print(errors.Wrap(err, "get next"))
				}

				continue
			}

			if !rcv.isTaskReadyForExecution(task) {
				shouldThrottle = true

				if err := rcv.storage.GetBack(ctx, task); err != nil {
					rcv.logger.Print(errors.Wrap(err, "get back"))
				}

				continue
			}

			shouldThrottle = false

			rcv.ch <- task
		}
		done <- true
	}()

	go func() {
		<-ctx.Done()
		shouldStop = true
	}()

	<-done
	rcv.close()

	return nil
}

func (rcv *Scheduler) runMissedTasksRestorer(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-ticker.C:
			if err := rcv.storage.RestoreFromBackup(ctx, rcv.getCurrentTime().Add(-1*missedTaskTimeout)); err != nil {
				rcv.logger.Print(errors.Wrap(err, "restore"))
			}
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func (rcv *Scheduler) isTaskReadyForExecution(task *Task) bool {
	return task.Time.Sub(rcv.getCurrentTime()) < maxTimeWindow
}

func (rcv *Scheduler) close() {
	if !rcv.done.CAS(false, true) { // one time execution
		return
	}

	close(rcv.ch)
	rcv.wg.Wait()
}

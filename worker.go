package tasks

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
)

type Worker struct {
	traceName      string
	callback       Callback
	storage        Storage
	getCurrentTime CurrentTimeResolver
	logger         *log.Logger
}

func NewWorker(
	callback Callback,
	storage Storage,
	getCurrentTime CurrentTimeResolver,
	logger *log.Logger,
) *Worker {
	return &Worker{
		callback:       callback,
		storage:        storage,
		getCurrentTime: getCurrentTime,
		logger:         logger,
	}
}

func (rcv *Worker) Do(ctx context.Context, ch <-chan *Task) {
	for task := range ch {
		rcv.waitForExecutionWindow(task, 100*time.Millisecond)

		err := rcv.callback(ctx, task)
		if err == nil {
			_ = rcv.storage.RemoveBackup(ctx, task.ID)
		} else {
			rcv.logger.Print(errors.Wrap(err, "evaluate task"))
		}
	}
}

func (rcv *Worker) waitForExecutionWindow(task *Task, delay time.Duration) {
	for {
		if task.Time.Sub(rcv.getCurrentTime()) <= 0 {
			break
		}
		time.Sleep(delay)
	}
}

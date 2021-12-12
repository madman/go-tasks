package tasks

import "github.com/pkg/errors"

var (
	ErrAddTaskFailed    = errors.New("add task failed")
	ErrRemoveTaskFailed = errors.New("remove task failed")
	ErrGetTaskFailed    = errors.New("get task failed")
	ErrNoTasks          = errors.New("no tasks")
)

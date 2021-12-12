package tasks

import (
	"context"
	"time"
)

type Storage interface {
	Add(ctx context.Context, Task *Task, at time.Time) error
	GetNext(ctx context.Context) (*Task, error)
	RemoveBackup(ctx context.Context, id string) error
	GetBack(ctx context.Context, task *Task) error
	RestoreFromBackup(ctx context.Context, earlyThen time.Time) error
	Remove(ctx context.Context, id string) error
}


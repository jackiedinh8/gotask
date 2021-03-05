package gotask

import (
	"fmt"
)

// ErrScheduleTimeout returned by Pool/TaskScheduler to indicate that there no free
// goroutines during some period of time.
var ErrScheduleTimeout = fmt.Errorf("schedule error: timed out")

// ErrNoWorker returned by Pool/TaskScheduler to indicate that there no available
// worker.
var ErrNoWorker = fmt.Errorf("No worker available")

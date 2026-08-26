package clock

import "time"

type Timer interface {
	C() <-chan time.Time
	Reset(time.Duration) bool
	Stop() bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type Real struct{}

func (Real) Now() time.Time {
	return time.Now()
}

func (Real) NewTimer(duration time.Duration) Timer {
	return &realTimer{timer: time.NewTimer(duration)}
}

type realTimer struct {
	timer *time.Timer
}

func (timer *realTimer) C() <-chan time.Time {
	return timer.timer.C
}

func (timer *realTimer) Reset(duration time.Duration) bool {
	return timer.timer.Reset(duration)
}

func (timer *realTimer) Stop() bool {
	return timer.timer.Stop()
}

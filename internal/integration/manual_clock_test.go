package integration

import (
	"sync"
	"time"

	queueclock "github.com/ElijahUmana/queuemaxxing/internal/clock"
)

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

type manualTimer struct {
	clock    *manualClock
	channel  chan time.Time
	deadline time.Time
	active   bool
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now, timers: make(map[*manualTimer]struct{})}
}

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualClock) NewTimer(duration time.Duration) queueclock.Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &manualTimer{clock: clock, channel: make(chan time.Time, 1), deadline: clock.now.Add(duration), active: true}
	clock.timers[timer] = struct{}{}
	clock.fireDueLocked()
	return timer
}

func (clock *manualClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
	clock.fireDueLocked()
}

func (clock *manualClock) fireDueLocked() {
	for timer := range clock.timers {
		if timer.active && !timer.deadline.After(clock.now) {
			timer.active = false
			select {
			case timer.channel <- clock.now:
			default:
			}
		}
	}
}

func (timer *manualTimer) C() <-chan time.Time { return timer.channel }

func (timer *manualTimer) Reset(duration time.Duration) bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	wasActive := timer.active
	timer.active = true
	timer.deadline = timer.clock.now.Add(duration)
	timer.clock.fireDueLocked()
	return wasActive
}

func (timer *manualTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	wasActive := timer.active
	timer.active = false
	return wasActive
}

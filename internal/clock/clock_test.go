package clock

import (
	"testing"
	"time"
)

func TestRealClockAndTimerLifecycle(t *testing.T) {
	clock := Real{}
	before := time.Now()
	now := clock.Now()
	if now.Before(before) || now.After(time.Now()) {
		t.Fatalf("Now() = %v outside expected range", now)
	}

	timer := clock.NewTimer(time.Hour)
	if timer.C() == nil {
		t.Fatal("timer channel is nil")
	}
	if !timer.Stop() {
		t.Fatal("new timer was already stopped")
	}
	if timer.Reset(time.Millisecond) {
		t.Fatal("reset of stopped timer reported active")
	}
	select {
	case <-timer.C():
	case <-time.After(time.Second):
		t.Fatal("reset timer did not fire")
	}
	if timer.Stop() {
		t.Fatal("expired timer reported active")
	}
}

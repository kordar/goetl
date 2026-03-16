package managed_source

import "time"

func waitDone(done <-chan struct{}, drain time.Duration) bool {
	if drain <= 0 {
		<-done
		return true
	}
	select {
	case <-done:
		return true
	case <-time.After(drain):
		return false
	}
}

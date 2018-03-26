package dummy

import (
	"log"
	"os"
)

// NewLogOutput returns a constructed SafeLogOuput and sets log output to it.
func NewLogOutput(out chan string) *SafeLogOutput {
	slo := &SafeLogOutput{
		done: make(chan struct{}),
		out:  out,
	}
	log.SetOutput(slo)
	return slo
}

// SafeLogOutput is a goroutine safe buffer for capturing
// log output.
type SafeLogOutput struct {
	done chan struct{}
	out  chan string
}

// Write writes the bytes to the buffer while locked.
func (s *SafeLogOutput) Write(p []byte) (int, error) {
	s.out <- string(p)
	return len(p), nil
}

// Reset returns the log output to stderr
func (s *SafeLogOutput) Reset() {
	log.SetOutput(os.Stderr)
	close(s.out)
}

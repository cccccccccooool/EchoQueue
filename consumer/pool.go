package consumer

import "sync"

// stageWorker is one goroutine of a stage pool. Retiring a worker closes its
// quit channel; the worker finishes whatever it is currently doing (a
// dispatcher cycle, a handled batch including its outcome hand-off, or a
// settle) and exits at the next loop boundary. A retiring worker never
// abandons a batch it already owns unless the run context is cancelled.
type stageWorker struct {
	quit chan struct{}
	done chan struct{}
}

// stagePool is a resizable goroutine pool for one pipeline stage. Growing
// spawns new workers; shrinking retires the most recently spawned workers
// and waits for them to finish. A pool is created per Run with the stage's
// loop function, so workers always capture the Run-local context and
// generation.
type stagePool struct {
	mu      sync.Mutex
	spawnFn func(quit <-chan struct{})
	workers []*stageWorker
}

func newStagePool(spawnFn func(quit <-chan struct{})) *stagePool {
	return &stagePool{spawnFn: spawnFn}
}

// resize grows or shrinks the pool to the target size. Shrinking blocks
// until the retired workers have exited; the pool therefore never contains
// goroutines that ignore their quit signal beyond their current unit of
// work.
func (p *stagePool) resize(target int) {
	p.mu.Lock()
	for len(p.workers) < target {
		worker := &stageWorker{quit: make(chan struct{}), done: make(chan struct{})}
		p.workers = append(p.workers, worker)
		go func() {
			defer close(worker.done)
			p.spawnFn(worker.quit)
		}()
	}
	retired := append([]*stageWorker(nil), p.workers[target:]...)
	p.workers = p.workers[:target]
	p.mu.Unlock()
	for _, worker := range retired {
		close(worker.quit)
	}
	for _, worker := range retired {
		<-worker.done
	}
}

// wait blocks until every worker currently registered in the pool has
// exited. Workers spawned after wait begins are not waited for; they exit on
// the run context or the stage signal channels and release their own tokens.
func (p *stagePool) wait() {
	p.mu.Lock()
	workers := append([]*stageWorker(nil), p.workers...)
	p.mu.Unlock()
	for _, worker := range workers {
		<-worker.done
	}
}

func (p *stagePool) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

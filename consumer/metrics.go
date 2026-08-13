package consumer

// MetricsSink receives host-visible stage counters. Implementations MUST be
// safe for concurrent use: dispatchers, handlers, and settlers call these
// methods from multiple goroutines. The reference host feeds them to expvar
// or Prometheus; the noop sink is the default so the pipeline never
// allocates for metrics that nobody consumes.
type MetricsSink interface {
	DispatchStarted()
	DispatchEmpty()
	DispatchFailed()
	BatchDispatched(taskCount int)
	HandleStarted()
	HandleDone()
	SettleStarted()
	SettleSucceeded()
	SettleFailed()
	BreakerOpened()
	BreakerClosed()
}

type noopMetrics struct{}

func (noopMetrics) DispatchStarted()    {}
func (noopMetrics) DispatchEmpty()      {}
func (noopMetrics) DispatchFailed()     {}
func (noopMetrics) BatchDispatched(int) {}
func (noopMetrics) HandleStarted()      {}
func (noopMetrics) HandleDone()         {}
func (noopMetrics) SettleStarted()      {}
func (noopMetrics) SettleSucceeded()    {}
func (noopMetrics) SettleFailed()       {}
func (noopMetrics) BreakerOpened()      {}
func (noopMetrics) BreakerClosed()      {}

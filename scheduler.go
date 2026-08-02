package echoqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

var (
	ErrRunAlreadyActive = errors.New("echoqueue: scheduler is already running")
)

// Scheduler owns one Redis client, one namespace, and immutable queue
// bindings. It does not read application configuration files.
type Scheduler struct {
	rdb    *redis.Client
	config Config
	keys   keyspace

	mu     sync.RWMutex
	queues map[string]*Queue

	runMu   sync.Mutex
	running bool

	redisCheckMu sync.Mutex
	redisReady   bool
	redisProbe   func(context.Context, *redis.Client) error
}

// Queue is an immutable binding of QueueConfig and the Scheduler defaults.
type Queue struct {
	scheduler *Scheduler
	config    QueueConfig
	settings  Config
}

type physicalRoute struct {
	source string
	result string
	dead   string
}

// New constructs a scheduler without contacting Redis. Redis capability is
// checked immediately before a Redis operation so ordinary unit tests do not
// require an external service.
func New(rdb *redis.Client, config Config) (*Scheduler, error) {
	if rdb == nil {
		return nil, errors.New("echoqueue: redis client is required")
	}
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		rdb:        rdb,
		config:     normalized,
		keys:       newKeyspace(normalized.Namespace),
		queues:     make(map[string]*Queue),
		redisProbe: checkRedis,
	}, nil
}

// Bind copies QueueConfig and the current Config defaults into an immutable
// Queue handle. Rebinding task names or physical Redis keys is rejected.
func (s *Scheduler) Bind(config QueueConfig) (*Queue, error) {
	if s == nil {
		return nil, errors.New("echoqueue: scheduler is nil")
	}
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	queue := &Queue{scheduler: s, config: normalized, settings: s.config}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.queues[normalized.TaskName]; exists {
		return nil, fmt.Errorf("echoqueue: task %q is already bound", normalized.TaskName)
	}
	route := s.physicalRoute(normalized)
	if err := validatePhysicalRoute(normalized.TaskName, route); err != nil {
		return nil, err
	}
	for existingTask, existingQueue := range s.queues {
		if err := routeConflict(normalized.TaskName, route, existingTask, s.physicalRoute(existingQueue.config)); err != nil {
			return nil, err
		}
	}
	s.queues[normalized.TaskName] = queue
	return queue, nil
}

func (s *Scheduler) physicalRoute(config QueueConfig) physicalRoute {
	result := config.Result
	if result == "" {
		result = s.keys.result(config.TaskName)
	}
	dead := config.Dead
	if dead == "" {
		dead = s.keys.deadFor(config.TaskName)
	}
	return physicalRoute{source: config.Source, result: result, dead: dead}
}

func validatePhysicalRoute(taskName string, route physicalRoute) error {
	fields := []struct {
		name  string
		value string
	}{
		{name: "source", value: route.source},
		{name: "result", value: route.result},
		{name: "dead", value: route.dead},
	}
	for i := 0; i < len(fields); i++ {
		for j := i + 1; j < len(fields); j++ {
			if fields[i].value == fields[j].value {
				return fmt.Errorf("echoqueue: task %q %s key %q conflicts with its %s key", taskName, fields[i].name, fields[i].value, fields[j].name)
			}
		}
	}
	return nil
}

func routeConflict(taskName string, route physicalRoute, existingTask string, existing physicalRoute) error {
	newFields := []struct {
		name  string
		value string
	}{
		{name: "source", value: route.source},
		{name: "result", value: route.result},
		{name: "dead", value: route.dead},
	}
	existingFields := []struct {
		name  string
		value string
	}{
		{name: "source", value: existing.source},
		{name: "result", value: existing.result},
		{name: "dead", value: existing.dead},
	}
	for _, newField := range newFields {
		for _, existingField := range existingFields {
			if newField.value == existingField.value {
				return fmt.Errorf("echoqueue: task %q %s key %q conflicts with task %q %s key", taskName, newField.name, newField.value, existingTask, existingField.name)
			}
		}
	}
	return nil
}

func (s *Scheduler) beginRun() bool {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if s.running {
		return false
	}
	s.running = true
	return true
}

func (s *Scheduler) endRun() {
	s.runMu.Lock()
	s.running = false
	s.runMu.Unlock()
}

func (s *Scheduler) ensureRedis(ctx context.Context) error {
	if s == nil || s.rdb == nil {
		return fmt.Errorf("echoqueue: redis capability check failed: nil scheduler")
	}
	if ctx == nil {
		return fmt.Errorf("echoqueue: redis capability check failed: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.redisCheckMu.Lock()
	defer s.redisCheckMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.redisReady {
		return nil
	}
	probe := s.redisProbe
	if probe == nil {
		probe = checkRedis
	}
	if err := probe(ctx, s.rdb); err != nil {
		return err
	}
	s.redisReady = true
	return nil
}

func (q *Queue) validate() error {
	if q == nil || q.scheduler == nil {
		return errors.New("echoqueue: queue is nil")
	}
	return nil
}

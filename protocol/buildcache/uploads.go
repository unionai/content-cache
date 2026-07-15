package buildcache

import (
	"context"
	"sync"
	"time"

	"github.com/buildkite/content-cache/telemetry"
)

type uploadKey struct {
	actionID string
	outputID string
}

type uploadEntry struct {
	generation uint64
	stopExpiry func() bool
}

type uploadLease struct {
	registry   *uploadRegistry
	key        uploadKey
	generation uint64
}

type uploadRegistry struct {
	mu             sync.Mutex
	entries        map[uploadKey]*uploadEntry
	nextGeneration uint64
	ttl            time.Duration
	scheduleExpiry func(time.Duration, func()) func() bool
}

func newUploadRegistry(
	ttl time.Duration,
	scheduleExpiry func(time.Duration, func()) func() bool,
) *uploadRegistry {
	if scheduleExpiry == nil {
		scheduleExpiry = func(after time.Duration, expire func()) func() bool {
			timer := time.AfterFunc(after, expire)
			return timer.Stop
		}
	}
	return &uploadRegistry{
		entries:        make(map[uploadKey]*uploadEntry),
		ttl:            ttl,
		scheduleExpiry: scheduleExpiry,
	}
}

func (r *uploadRegistry) acquire(actionID, outputID string) (*uploadLease, bool) {
	key := uploadKey{actionID: actionID, outputID: outputID}

	r.mu.Lock()
	if _, ok := r.entries[key]; ok {
		r.mu.Unlock()
		return nil, false
	}

	r.nextGeneration++
	generation := r.nextGeneration
	entry := &uploadEntry{generation: generation}
	r.entries[key] = entry
	entry.stopExpiry = r.scheduleExpiry(r.ttl, func() {
		r.remove(key, generation, false)
	})
	r.mu.Unlock()
	telemetry.AddBuildCacheUploadsInflight(context.Background(), 1)

	return &uploadLease{registry: r, key: key, generation: generation}, true
}

func (l *uploadLease) release() {
	if l == nil {
		return
	}
	l.registry.remove(l.key, l.generation, true)
}

func (r *uploadRegistry) remove(key uploadKey, generation uint64, stopExpiry bool) {
	r.mu.Lock()
	entry, ok := r.entries[key]
	if !ok || entry.generation != generation {
		r.mu.Unlock()
		return
	}
	delete(r.entries, key)
	if stopExpiry && entry.stopExpiry != nil {
		entry.stopExpiry()
	}
	r.mu.Unlock()
	telemetry.AddBuildCacheUploadsInflight(context.Background(), -1)
}

package buildcache

import (
	"sync"
	"time"
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
	mu              sync.Mutex
	entries         map[uploadKey]*uploadEntry
	loadingByAction map[string]int
	nextGeneration  uint64
	ttl             time.Duration
	scheduleExpiry  func(time.Duration, func()) func() bool
	onSizeChanged   func(int)
}

func newUploadRegistry(
	ttl time.Duration,
	scheduleExpiry func(time.Duration, func()) func() bool,
	onSizeChanged func(int),
) *uploadRegistry {
	if scheduleExpiry == nil {
		scheduleExpiry = func(after time.Duration, expire func()) func() bool {
			timer := time.AfterFunc(after, expire)
			return timer.Stop
		}
	}
	return &uploadRegistry{
		entries:         make(map[uploadKey]*uploadEntry),
		loadingByAction: make(map[string]int),
		ttl:             ttl,
		scheduleExpiry:  scheduleExpiry,
		onSizeChanged:   onSizeChanged,
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
	r.loadingByAction[actionID]++
	entry.stopExpiry = r.scheduleExpiry(r.ttl, func() {
		r.remove(key, generation, false)
	})
	size := len(r.entries)
	r.notifySizeChanged(size)
	r.mu.Unlock()

	return &uploadLease{registry: r, key: key, generation: generation}, true
}

func (r *uploadRegistry) isLoading(actionID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loadingByAction[actionID] > 0
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
	if r.loadingByAction[key.actionID] == 1 {
		delete(r.loadingByAction, key.actionID)
	} else {
		r.loadingByAction[key.actionID]--
	}
	if stopExpiry && entry.stopExpiry != nil {
		entry.stopExpiry()
	}
	size := len(r.entries)
	r.notifySizeChanged(size)
	r.mu.Unlock()
}

func (r *uploadRegistry) notifySizeChanged(size int) {
	if r.onSizeChanged != nil {
		r.onSizeChanged(size)
	}
}

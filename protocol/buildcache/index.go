package buildcache

import (
	"context"
	"errors"

	"github.com/buildkite/content-cache/store/metadb"
)

// Index manages the actionID → blob mapping using metadb envelope storage.
type Index struct {
	entries *metadb.EnvelopeIndex // protocol="buildcache", kind="entry"
}

// NewIndex creates a new build cache index backed by the given envelope index.
func NewIndex(entries *metadb.EnvelopeIndex) *Index {
	return &Index{entries: entries}
}

// Get retrieves the entry for the given actionID.
func (idx *Index) Get(ctx context.Context, actionID string) (*ActionEntry, error) {
	var entry ActionEntry
	if err := idx.entries.GetJSON(ctx, actionID, &entry); err != nil {
		if errors.Is(err, metadb.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &entry, nil
}

// Put stores an entry for the given actionID.
//
// Build cache entries deliberately do not pin their blobs. S3-FIFO and full GC
// may reclaim build artifacts; a mapping whose blob has been removed is treated
// as a cache miss and later removed by normal TTL expiry.
func (idx *Index) Put(ctx context.Context, actionID string, entry *ActionEntry) error {
	return idx.entries.PutJSON(ctx, actionID, entry, nil)
}

package buildcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUploadRegistryCoalescesOnlyMatchingOutputs(t *testing.T) {
	registry := newUploadRegistry(time.Minute, nil)

	first, leader := registry.acquire("action", "output-1")
	require.True(t, leader)

	_, leader = registry.acquire("action", "output-1")
	require.False(t, leader)

	second, leader := registry.acquire("action", "output-2")
	require.True(t, leader)

	first.release()
	first, leader = registry.acquire("action", "output-1")
	require.True(t, leader)

	second.release()
	first.release()
}

func TestUploadRegistryExpiryIsGenerationSafe(t *testing.T) {
	var expiries []func()
	schedule := func(_ time.Duration, expire func()) func() bool {
		expiries = append(expiries, expire)
		return func() bool { return true }
	}
	registry := newUploadRegistry(time.Minute, schedule)

	first, leader := registry.acquire("action", "output")
	require.True(t, leader)
	first.release()

	second, leader := registry.acquire("action", "output")
	require.True(t, leader)
	require.Len(t, expiries, 2)

	// A stopped timer may already have begun running. Its callback must not
	// delete a newer generation for the same action and output.
	expiries[0]()
	_, leader = registry.acquire("action", "output")
	require.False(t, leader)

	expiries[1]()
	third, leader := registry.acquire("action", "output")
	require.True(t, leader)
	third.release()
	second.release()
}

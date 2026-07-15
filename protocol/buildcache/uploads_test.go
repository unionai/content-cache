package buildcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUploadRegistryCoalescesOnlyMatchingOutputs(t *testing.T) {
	registry := newUploadRegistry(time.Minute, nil, nil)

	first, leader := registry.acquire("action", "output-1")
	require.True(t, leader)
	require.True(t, registry.isLoading("action"))

	_, leader = registry.acquire("action", "output-1")
	require.False(t, leader)

	second, leader := registry.acquire("action", "output-2")
	require.True(t, leader)

	first.release()
	require.True(t, registry.isLoading("action"))

	second.release()
	require.False(t, registry.isLoading("action"))
}

func TestUploadRegistryExpiryIsGenerationSafe(t *testing.T) {
	var expiries []func()
	schedule := func(_ time.Duration, expire func()) func() bool {
		expiries = append(expiries, expire)
		return func() bool { return true }
	}
	registry := newUploadRegistry(time.Minute, schedule, nil)

	first, leader := registry.acquire("action", "output")
	require.True(t, leader)
	first.release()

	second, leader := registry.acquire("action", "output")
	require.True(t, leader)
	require.Len(t, expiries, 2)

	// A stopped timer may already have begun running. Its callback must not
	// delete a newer generation for the same action and output.
	expiries[0]()
	require.True(t, registry.isLoading("action"))

	expiries[1]()
	require.False(t, registry.isLoading("action"))
	second.release()
}

func TestUploadRegistryReportsInflightDeltas(t *testing.T) {
	var deltas []int
	registry := newUploadRegistry(time.Minute, nil, func(delta int) {
		deltas = append(deltas, delta)
	})

	lease, leader := registry.acquire("action", "output")
	require.True(t, leader)
	lease.release()

	require.Equal(t, []int{1, -1}, deltas)
}

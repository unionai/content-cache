package git

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpstreamBasicAuthProviderFailureFailsClosed(t *testing.T) {
	var upstreamRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequests++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	client := redirectClient(upstream.URL)
	u := NewUpstream(
		WithHTTPClient(client),
		WithBasicAuthProvider(failingBasicAuthProvider{}),
	)

	rc, _, err := u.FetchInfoRefs(context.Background(), RepoRef{Host: "github.com", RepoPath: "buildkite/content-cache"}, "")
	require.Error(t, err)
	require.Nil(t, rc)
	require.Contains(t, err.Error(), "resolving upstream auth")
	require.Zero(t, upstreamRequests, "auth provider failure must not fall through to unauthenticated upstream")
}

func TestUpstreamUsesBasicAuthProvider(t *testing.T) {
	var authHeader string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", ContentTypeUploadPackAdvertisement)
		_, _ = w.Write([]byte(fakeInfoRefsBody))
	}))
	t.Cleanup(upstream.Close)

	u := NewUpstream(
		WithHTTPClient(redirectClient(upstream.URL)),
		WithBasicAuthProvider(staticBasicAuthProvider{username: "x-access-token", password: "token"}),
	)

	rc, _, err := u.FetchInfoRefs(context.Background(), RepoRef{Host: "github.com", RepoPath: "buildkite/content-cache"}, "")
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.True(t, strings.HasPrefix(authHeader, "Basic "))
}

func TestUpstreamUploadPackBasicAuthProviderFailureFailsClosed(t *testing.T) {
	var upstreamRequests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamRequests++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	u := NewUpstream(
		WithHTTPClient(redirectClient(upstream.URL)),
		WithBasicAuthProvider(failingBasicAuthProvider{}),
	)

	rc, err := u.FetchUploadPack(context.Background(), RepoRef{Host: "github.com", RepoPath: "buildkite/content-cache"}, "", strings.NewReader("body"))
	require.Error(t, err)
	require.Nil(t, rc)
	require.Contains(t, err.Error(), "resolving upstream auth")
	require.Zero(t, upstreamRequests, "auth provider failure must not fall through to unauthenticated upstream")
}

type failingBasicAuthProvider struct{}

func (failingBasicAuthProvider) BasicAuth(context.Context, RepoRef) (string, string, error) {
	return "", "", fmt.Errorf("boom")
}

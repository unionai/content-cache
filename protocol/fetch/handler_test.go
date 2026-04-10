package fetch

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/buildkite/content-cache/backend"
	"github.com/buildkite/content-cache/store"
	"github.com/buildkite/content-cache/store/metadb"
)

func setupMetaDB(t *testing.T, tmpDir string) (*metadb.BoltDB, *Index) {
	t.Helper()
	db := metadb.NewBoltDB(metadb.WithNoSync(true))
	require.NoError(t, db.Open(filepath.Join(tmpDir, "meta.db")))
	t.Cleanup(func() { _ = db.Close() })

	resources, err := metadb.NewEnvelopeIndex(db, "fetch", "resource", 24*time.Hour)
	require.NoError(t, err)
	return db, NewIndex(resources)
}

func newTestHandler(t *testing.T, client *http.Client, opts ...HandlerOption) *Handler {
	t.Helper()
	tmpDir := t.TempDir()
	b, err := backend.NewFilesystem(tmpDir)
	require.NoError(t, err)
	cafs := store.NewCAFS(b)

	_, idx := setupMetaDB(t, tmpDir)
	baseOpts := []HandlerOption{}
	if client != nil {
		baseOpts = append(baseOpts, WithHTTPClient(client))
	}
	baseOpts = append(baseOpts, opts...)
	return NewHandler(idx, cafs, baseOpts...)
}

func TestHandlerFetchCachesResource(t *testing.T) {
	requests := 0
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/tools/tool/v1.0.0/tool.tar.gz" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/tools/tool/v1.0.0/tool.tar.gz")
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Disposition", `attachment; filename="tool.tar.gz"`)
		w.Header().Set("ETag", `"abc123"`)
		_, _ = w.Write([]byte("tool-content"))
	}))
	defer upstream.Close()

	host := upstream.Listener.Addr().String()
	h := newTestHandler(t, upstream.Client(), WithAllowedHosts([]string{host}))

	req := httptest.NewRequest(http.MethodGet, "/"+host+"/tools/tool/v1.0.0/tool.tar.gz", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "tool-content", w.Body.String())
	require.Equal(t, "application/gzip", w.Header().Get("Content-Type"))
	require.Equal(t, `attachment; filename="tool.tar.gz"`, w.Header().Get("Content-Disposition"))
	require.Equal(t, `"abc123"`, w.Header().Get("ETag"))

	req2 := httptest.NewRequest(http.MethodGet, "/"+host+"/tools/tool/v1.0.0/tool.tar.gz", nil)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)

	require.Equal(t, http.StatusOK, w2.Code)
	require.Equal(t, "tool-content", w2.Body.String())
	require.Equal(t, 1, requests)
}

func TestHandlerGitHubReleaseCachesRedirectedAsset(t *testing.T) {
	assetRequests := 0
	assetServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assetRequests++
		if r.URL.Path != "/download/tool.tar.gz" {
			t.Errorf("asset path = %q, want %q", r.URL.Path, "/download/tool.tar.gz")
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write([]byte("redirected-asset"))
	}))
	defer assetServer.Close()

	githubRequests := 0
	githubServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		githubRequests++
		if r.URL.Path != "/owner/repo/releases/download/v1.0.0/tool.tar.gz" {
			t.Errorf("github release path = %q, want %q", r.URL.Path, "/owner/repo/releases/download/v1.0.0/tool.tar.gz")
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		http.Redirect(w, r, assetServer.URL+"/download/tool.tar.gz", http.StatusFound)
	}))
	defer githubServer.Close()

	pool := x509.NewCertPool()
	pool.AddCert(assetServer.Certificate())
	pool.AddCert(githubServer.Certificate())
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
		Timeout:   defaultTimeout,
	}

	h := newTestHandler(
		t,
		client,
		WithGitHubReleaseHost(githubServer.Listener.Addr().String()),
		WithGitHubReleaseRedirectHosts(assetServer.Listener.Addr().String()),
	)

	req := httptest.NewRequest(http.MethodGet, "/owner/repo/releases/download/v1.0.0/tool.tar.gz", nil)
	w := httptest.NewRecorder()
	h.ServeGitHubRelease(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "redirected-asset", w.Body.String())
	require.Equal(t, 1, githubRequests)
	require.Equal(t, 1, assetRequests)

	req2 := httptest.NewRequest(http.MethodGet, "/owner/repo/releases/download/v1.0.0/tool.tar.gz", nil)
	w2 := httptest.NewRecorder()
	h.ServeGitHubRelease(w2, req2)

	require.Equal(t, http.StatusOK, w2.Code)
	require.Equal(t, "redirected-asset", w2.Body.String())
	require.Equal(t, 1, githubRequests)
	require.Equal(t, 1, assetRequests)
}

func TestHandlerFetchRejectsDisallowedHost(t *testing.T) {
	h := newTestHandler(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/example.com/tool.tar.gz", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code)
}

func TestHandlerFetchRejectsUnlistedPortOnAllowedHostname(t *testing.T) {
	requests := 0
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_, _ = w.Write([]byte("should-not-be-fetched"))
	}))
	defer upstream.Close()

	host, _, err := net.SplitHostPort(upstream.Listener.Addr().String())
	require.NoError(t, err)

	h := newTestHandler(t, upstream.Client(), WithAllowedHosts([]string{host}))
	req := httptest.NewRequest(http.MethodGet, "/"+upstream.Listener.Addr().String()+"/tool.tar.gz", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code)
	require.Zero(t, requests)
}

func TestHandlerFetchPreservesContentEncodingOnHeadMiss(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/encoded/tool.tar.gz" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/encoded/tool.tar.gz")
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Encoding", "br")
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write([]byte("brotli-bytes"))
	}))
	defer upstream.Close()

	h := newTestHandler(t, upstream.Client(), WithAllowedHosts([]string{upstream.Listener.Addr().String()}))
	req := httptest.NewRequest(http.MethodHead, "/"+upstream.Listener.Addr().String()+"/encoded/tool.tar.gz", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "br", w.Header().Get("Content-Encoding"))
}

func TestHandlerFetchPreservesContentEncodingOnMissAndHit(t *testing.T) {
	requests := 0
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/encoded/tool.tar.gz" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/encoded/tool.tar.gz")
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Encoding", "br")
		_, _ = w.Write([]byte("brotli-bytes"))
	}))
	defer upstream.Close()

	h := newTestHandler(t, upstream.Client(), WithAllowedHosts([]string{upstream.Listener.Addr().String()}))

	firstReq := httptest.NewRequest(http.MethodGet, "/"+upstream.Listener.Addr().String()+"/encoded/tool.tar.gz", nil)
	firstRes := httptest.NewRecorder()
	h.ServeHTTP(firstRes, firstReq)

	require.Equal(t, http.StatusOK, firstRes.Code)
	require.Equal(t, "br", firstRes.Header().Get("Content-Encoding"))
	require.Equal(t, "brotli-bytes", firstRes.Body.String())

	secondReq := httptest.NewRequest(http.MethodGet, "/"+upstream.Listener.Addr().String()+"/encoded/tool.tar.gz", nil)
	secondRes := httptest.NewRecorder()
	h.ServeHTTP(secondRes, secondReq)

	require.Equal(t, http.StatusOK, secondRes.Code)
	require.Equal(t, "br", secondRes.Header().Get("Content-Encoding"))
	require.Equal(t, "brotli-bytes", secondRes.Body.String())
	require.Equal(t, 1, requests)
}

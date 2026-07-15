package buildcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildkite/content-cache/backend"
	"github.com/buildkite/content-cache/store"
	"github.com/buildkite/content-cache/store/metadb"
	"github.com/stretchr/testify/require"
)

type readTrackingBody struct {
	reader io.Reader
	reads  int
}

type blockingBody struct {
	reader  io.Reader
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingBody(body string) *blockingBody {
	return &blockingBody{
		reader:  strings.NewReader(body),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (b *blockingBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return b.reader.Read(p)
}

type errorBody struct{ err error }

func (b errorBody) Read([]byte) (int, error) { return 0, b.err }

func (b *readTrackingBody) Read(p []byte) (int, error) {
	b.reads++
	return b.reader.Read(p)
}

func (b *readTrackingBody) readCount() int { return b.reads }

func newTestHandler(t *testing.T) (*Handler, *Index, store.Store) {
	t.Helper()

	tmpDir := t.TempDir()
	b, err := backend.NewFilesystem(tmpDir)
	require.NoError(t, err)

	db := metadb.NewBoltDB()
	dbPath := filepath.Join(tmpDir, "metadata.db")
	err = db.Open(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	entryIndex, err := metadb.NewEnvelopeIndex(db, "buildcache", "entry", 24*time.Hour)
	require.NoError(t, err)

	idx := NewIndex(entryIndex)
	cafsStore := store.NewCAFS(b)
	handler := NewHandler(idx, cafsStore)

	return handler, idx, cafsStore
}

func TestHandlerGetMiss(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerPutThenGet(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	blobContent := "hello build cache"
	actionID := "aa" + strings.Repeat("00", 31)
	outputID := "bb" + strings.Repeat("00", 31)

	// PUT the blob.
	putReq := httptest.NewRequest(http.MethodPut, "/"+actionID+"?output_id="+outputID, bytes.NewReader([]byte(blobContent)))
	putRec := httptest.NewRecorder()
	handler.ServeHTTP(putRec, putReq)
	require.Equal(t, http.StatusNoContent, putRec.Code)

	// GET the blob.
	getReq := httptest.NewRequest(http.MethodGet, "/"+actionID, nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)

	require.Equal(t, http.StatusOK, getRec.Code)
	require.Equal(t, outputID, getRec.Header().Get("X-Output-ID"))
	body, _ := io.ReadAll(getRec.Body)
	require.Equal(t, blobContent, string(body))
	require.Equal(t, "17", getRec.Header().Get("Content-Length"))
}

func TestHandlerInFlightFollowerReturnsNoContentWithoutReadingBody(t *testing.T) {
	handler, _, _ := newTestHandler(t)
	actionID := "ca" + strings.Repeat("00", 31)
	outputID := "cb" + strings.Repeat("00", 31)
	leaderBody := newBlockingBody("artifact")

	leaderDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPut, "/"+actionID+"?output_id="+outputID, leaderBody)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		leaderDone <- rec
	}()
	<-leaderBody.started

	followerBody := &readTrackingBody{reader: strings.NewReader("artifact")}
	followerReq := httptest.NewRequest(http.MethodPut, "/"+actionID+"?output_id="+outputID, followerBody)
	followerRec := httptest.NewRecorder()
	handler.ServeHTTP(followerRec, followerReq)

	getWhileLoading := httptest.NewRecorder()
	handler.ServeHTTP(getWhileLoading, httptest.NewRequest(http.MethodGet, "/"+actionID, nil))
	close(leaderBody.release)
	leaderRec := <-leaderDone

	getAfterSuccess := httptest.NewRecorder()
	handler.ServeHTTP(getAfterSuccess, httptest.NewRequest(http.MethodGet, "/"+actionID, nil))

	require.Equal(t, http.StatusNoContent, followerRec.Code)
	require.Zero(t, followerBody.readCount())
	require.Equal(t, http.StatusNotFound, getWhileLoading.Code)
	require.Equal(t, http.StatusNoContent, leaderRec.Code)
	require.Equal(t, http.StatusOK, getAfterSuccess.Code)
}

func TestHandlerAlreadyLoadedPutDoesNotReadBody(t *testing.T) {
	handler, _, _ := newTestHandler(t)
	actionID := "da" + strings.Repeat("00", 31)
	outputID := "db" + strings.Repeat("00", 31)

	firstRec := httptest.NewRecorder()
	handler.ServeHTTP(firstRec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+outputID,
		strings.NewReader("artifact"),
	))
	require.Equal(t, http.StatusNoContent, firstRec.Code)

	body := &readTrackingBody{reader: strings.NewReader("artifact")}
	loadedRec := httptest.NewRecorder()
	handler.ServeHTTP(loadedRec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+outputID,
		body,
	))

	require.Equal(t, http.StatusNoContent, loadedRec.Code)
	require.Zero(t, body.readCount())
}

func TestHandlerStaleMappingDoesNotUseLoadedFastPath(t *testing.T) {
	handler, idx, _ := newTestHandler(t)
	actionID := "ea" + strings.Repeat("00", 31)
	outputID := "eb" + strings.Repeat("00", 31)
	require.NoError(t, idx.Put(t.Context(), actionID, &ActionEntry{
		OutputID: outputID,
		BlobHash: "blake3:" + strings.Repeat("cc", 32),
		Size:     8,
	}))

	body := &readTrackingBody{reader: strings.NewReader("artifact")}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+outputID,
		body,
	))

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.NotZero(t, body.readCount())
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/"+actionID, nil))
	require.Equal(t, http.StatusOK, getRec.Code)
}

func TestHandlerLeaderFailureAllowsRetry(t *testing.T) {
	handler, idx, _ := newTestHandler(t)
	actionID := "ac" + strings.Repeat("00", 31)
	outputID := "ad" + strings.Repeat("00", 31)

	failedRec := httptest.NewRecorder()
	handler.ServeHTTP(failedRec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+outputID,
		errorBody{err: errors.New("injected read failure")},
	))
	_, indexErr := idx.Get(t.Context(), actionID)

	retryRec := httptest.NewRecorder()
	handler.ServeHTTP(retryRec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+outputID,
		strings.NewReader("artifact"),
	))

	require.Equal(t, http.StatusInternalServerError, failedRec.Code)
	require.ErrorIs(t, indexErr, ErrNotFound)
	require.Equal(t, http.StatusNoContent, retryRec.Code)
}

func TestHandlerMissingActionID(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "missing action ID")
}

func TestHandlerInvalidActionID(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	tests := []struct {
		name     string
		actionID string
	}{
		{"not hex", "/zzzzzzzz"},
		{"odd length", "/abc"},
		{"path traversal", "/..%2f..%2fetc%2fpasswd"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.actionID, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/aa00", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandlerPutMissingOutputID(t *testing.T) {
	handler, _, _ := newTestHandler(t)

	actionID := "aa" + strings.Repeat("00", 31)
	req := httptest.NewRequest(http.MethodPut, "/"+actionID, bytes.NewReader([]byte("data")))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "missing output_id")
}

func TestIndexGetPut(t *testing.T) {
	tmpDir := t.TempDir()
	db := metadb.NewBoltDB()
	err := db.Open(filepath.Join(tmpDir, "meta.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	entryIndex, err := metadb.NewEnvelopeIndex(db, "buildcache", "entry", 24*time.Hour)
	require.NoError(t, err)
	idx := NewIndex(entryIndex)

	ctx := context.Background()

	// Get non-existent.
	_, err = idx.Get(ctx, "missing")
	require.ErrorIs(t, err, ErrNotFound)

	// Put and get.
	blobHash := "blake3:" + strings.Repeat("cc", 32)
	entry := &ActionEntry{
		OutputID: strings.Repeat("bb", 32),
		BlobHash: blobHash,
		Size:     42,
	}
	require.NoError(t, idx.Put(ctx, "aa00", entry))

	got, err := idx.Get(ctx, "aa00")
	require.NoError(t, err)
	require.Equal(t, entry.OutputID, got.OutputID)
	require.Equal(t, entry.BlobHash, got.BlobHash)
	require.Equal(t, entry.Size, got.Size)

	refs, err := entryIndex.GetBlobRefs(ctx, "aa00")
	require.NoError(t, err)
	require.Equal(t, []string{blobHash}, refs)
}

func TestIsValidHex(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"aa", true},
		{"aabb", true},
		{"AABB", true},
		{"", false},
		{"a", false},   // odd length
		{"zz", false},  // not hex
		{"abc", false}, // odd length
		{"0123456789abcdef", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.want, isValidHex(tt.input))
		})
	}
}

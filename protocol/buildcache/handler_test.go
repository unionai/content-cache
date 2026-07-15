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

type writePlan struct {
	started         chan struct{}
	release         chan struct{}
	err             error
	ignoreCancelled bool
}

type writeGateBackend struct {
	backend.Backend
	mu     sync.Mutex
	plans  []*writePlan
	writes int
}

func (b *writeGateBackend) blockNextWrite(err error) (<-chan struct{}, chan<- struct{}) {
	return b.planNextWrite(err, false)
}

func (b *writeGateBackend) blockNextWriteIgnoringCancellation(err error) (<-chan struct{}, chan<- struct{}) {
	return b.planNextWrite(err, true)
}

func (b *writeGateBackend) planNextWrite(err error, ignoreCancelled bool) (<-chan struct{}, chan<- struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	plan := &writePlan{
		started:         make(chan struct{}),
		release:         make(chan struct{}),
		err:             err,
		ignoreCancelled: ignoreCancelled,
	}
	b.plans = append(b.plans, plan)
	return plan.started, plan.release
}

func (b *writeGateBackend) Write(ctx context.Context, key string, r io.Reader) error {
	b.mu.Lock()
	b.writes++
	var plan *writePlan
	if len(b.plans) > 0 {
		plan = b.plans[0]
		b.plans = b.plans[1:]
	}
	b.mu.Unlock()

	if plan != nil {
		close(plan.started)
		if plan.ignoreCancelled {
			<-plan.release
		} else {
			select {
			case <-plan.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if plan.err != nil {
			return plan.err
		}
	}
	return b.Backend.Write(ctx, key, r)
}

func (b *writeGateBackend) writeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.writes
}

type recordingEvictionNotifier struct {
	mu         sync.Mutex
	admissions int
}

func (n *recordingEvictionNotifier) Admit(context.Context, string, int64) {
	n.mu.Lock()
	n.admissions++
	n.mu.Unlock()
}

func (*recordingEvictionNotifier) Remove(context.Context, string, int64) {}

func (n *recordingEvictionNotifier) admissionCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.admissions
}

type readTrackingBody struct {
	reader io.Reader
	mu     sync.Mutex
	reads  int
}

type panicBody struct{}

func (panicBody) Read([]byte) (int, error) {
	panic("injected body read panic")
}

func (b *readTrackingBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	b.reads++
	b.mu.Unlock()
	return b.reader.Read(p)
}

func (b *readTrackingBody) readCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reads
}

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

func newGatedTestHandler(t *testing.T) (*Handler, *Index, *writeGateBackend, *recordingEvictionNotifier) {
	t.Helper()

	tmpDir := t.TempDir()
	filesystem, err := backend.NewFilesystem(tmpDir)
	require.NoError(t, err)
	gatedBackend := &writeGateBackend{Backend: filesystem}

	db := metadb.NewBoltDB()
	require.NoError(t, db.Open(filepath.Join(tmpDir, "metadata.db")))
	t.Cleanup(func() { _ = db.Close() })

	entryIndex, err := metadb.NewEnvelopeIndex(db, "buildcache", "entry", 24*time.Hour)
	require.NoError(t, err)
	idx := NewIndex(entryIndex)
	notifier := &recordingEvictionNotifier{}
	cafsStore := store.NewCAFS(
		gatedBackend,
		store.WithMetaDB(db),
		store.WithEvictionNotifier(notifier),
	)

	return NewHandler(idx, cafsStore), idx, gatedBackend, notifier
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
	handler, _, gatedBackend, notifier := newGatedTestHandler(t)
	actionID := "ca" + strings.Repeat("00", 31)
	outputID := "cb" + strings.Repeat("00", 31)
	started, release := gatedBackend.blockNextWrite(nil)

	leaderDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPut, "/"+actionID+"?output_id="+outputID, strings.NewReader("artifact"))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		leaderDone <- rec
	}()
	<-started

	followerBody := &readTrackingBody{reader: strings.NewReader("artifact")}
	followerReq := httptest.NewRequest(http.MethodPut, "/"+actionID+"?output_id="+outputID, followerBody)
	followerRec := httptest.NewRecorder()
	handler.ServeHTTP(followerRec, followerReq)

	getWhileLoading := httptest.NewRecorder()
	handler.ServeHTTP(getWhileLoading, httptest.NewRequest(http.MethodGet, "/"+actionID, nil))
	writesBeforeRelease := gatedBackend.writeCount()
	admissionsBeforeRelease := notifier.admissionCount()
	close(release)
	leaderRec := <-leaderDone

	getAfterSuccess := httptest.NewRecorder()
	handler.ServeHTTP(getAfterSuccess, httptest.NewRequest(http.MethodGet, "/"+actionID, nil))

	require.Equal(t, http.StatusNoContent, followerRec.Code)
	require.Zero(t, followerBody.readCount())
	require.Equal(t, 1, writesBeforeRelease)
	require.Zero(t, admissionsBeforeRelease)
	require.Equal(t, http.StatusNotFound, getWhileLoading.Code)
	require.Equal(t, http.StatusNoContent, leaderRec.Code)
	require.Equal(t, http.StatusOK, getAfterSuccess.Code)
	require.Equal(t, 1, notifier.admissionCount())
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

func TestHandlerDifferentOutputsAreNotCoalesced(t *testing.T) {
	handler, _, gatedBackend, _ := newGatedTestHandler(t)
	actionID := "fa" + strings.Repeat("00", 31)
	firstOutputID := "fb" + strings.Repeat("00", 31)
	secondOutputID := "fc" + strings.Repeat("00", 31)
	started, release := gatedBackend.blockNextWrite(nil)

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(
			http.MethodPut,
			"/"+actionID+"?output_id="+firstOutputID,
			strings.NewReader("first artifact"),
		))
		firstDone <- rec
	}()
	<-started

	secondBody := &readTrackingBody{reader: strings.NewReader("second artifact")}
	secondRec := httptest.NewRecorder()
	handler.ServeHTTP(secondRec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+secondOutputID,
		secondBody,
	))
	close(release)
	firstRec := <-firstDone

	require.Equal(t, http.StatusNoContent, secondRec.Code)
	require.NotZero(t, secondBody.readCount())
	require.Equal(t, 2, gatedBackend.writeCount())
	require.Equal(t, http.StatusNoContent, firstRec.Code)
}

func TestHandlerGetMissesWhileReplacementOutputLoads(t *testing.T) {
	handler, _, gatedBackend, _ := newGatedTestHandler(t)
	actionID := "ce" + strings.Repeat("00", 31)
	loadedOutputID := "cf" + strings.Repeat("00", 31)
	loadingOutputID := "de" + strings.Repeat("00", 31)

	loadedRec := httptest.NewRecorder()
	handler.ServeHTTP(loadedRec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+loadedOutputID,
		strings.NewReader("loaded artifact"),
	))
	require.Equal(t, http.StatusNoContent, loadedRec.Code)

	started, release := gatedBackend.blockNextWrite(nil)
	replacementDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(
			http.MethodPut,
			"/"+actionID+"?output_id="+loadingOutputID,
			strings.NewReader("replacement artifact"),
		))
		replacementDone <- rec
	}()
	<-started

	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/"+actionID, nil))
	close(release)
	replacementRec := <-replacementDone

	require.Equal(t, http.StatusNotFound, getRec.Code)
	require.Equal(t, http.StatusNoContent, replacementRec.Code)
}

func TestHandlerLeaderFailureAllowsRetry(t *testing.T) {
	handler, idx, gatedBackend, _ := newGatedTestHandler(t)
	actionID := "ac" + strings.Repeat("00", 31)
	outputID := "ad" + strings.Repeat("00", 31)
	started, release := gatedBackend.blockNextWrite(errors.New("injected write failure"))

	failedDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(
			http.MethodPut,
			"/"+actionID+"?output_id="+outputID,
			strings.NewReader("artifact"),
		))
		failedDone <- rec
	}()
	<-started
	close(release)
	failedRec := <-failedDone
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

func TestHandlerCancellationClearsRegistration(t *testing.T) {
	handler, _, gatedBackend, _ := newGatedTestHandler(t)
	actionID := "bc" + strings.Repeat("00", 31)
	outputID := "bd" + strings.Repeat("00", 31)
	started, release := gatedBackend.blockNextWriteIgnoringCancellation(nil)

	sizes := make(chan int, 4)
	handler.uploads = newUploadRegistry(uploadRegistrationTTL, nil, func(size int) { sizes <- size })
	ctx, cancel := context.WithCancel(t.Context())
	cancelledDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(
			http.MethodPut,
			"/"+actionID+"?output_id="+outputID,
			strings.NewReader("artifact"),
		).WithContext(ctx)
		handler.ServeHTTP(rec, req)
		cancelledDone <- rec
	}()
	<-started
	<-sizes // leader registration
	cancel()
	require.Zero(t, <-sizes) // cancellation cleanup

	retryRec := httptest.NewRecorder()
	handler.ServeHTTP(retryRec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+outputID,
		strings.NewReader("artifact"),
	))
	close(release)
	cancelledRec := <-cancelledDone

	require.Equal(t, http.StatusNoContent, retryRec.Code)
	require.Equal(t, http.StatusInternalServerError, cancelledRec.Code)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/"+actionID, nil))
	require.Equal(t, http.StatusOK, getRec.Code)
}

func TestHandlerPanicClearsRegistration(t *testing.T) {
	handler, _, _ := newTestHandler(t)
	actionID := "be" + strings.Repeat("00", 31)
	outputID := "bf" + strings.Repeat("00", 31)

	panicResult := make(chan any, 1)
	go func() {
		defer func() { panicResult <- recover() }()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
			http.MethodPut,
			"/"+actionID+"?output_id="+outputID,
			panicBody{},
		))
	}()
	require.Equal(t, "injected body read panic", <-panicResult)

	retryRec := httptest.NewRecorder()
	handler.ServeHTTP(retryRec, httptest.NewRequest(
		http.MethodPut,
		"/"+actionID+"?output_id="+outputID,
		strings.NewReader("artifact"),
	))
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

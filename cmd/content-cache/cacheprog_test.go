package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestRunner creates a cacheprogRunner wired to a test HTTP server
// and local temp directory, with stdout captured to a buffer.
func newTestRunner(t *testing.T, serverURL string) (*cacheprogRunner, *bytes.Buffer) {
	t.Helper()
	localDir := t.TempDir()
	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	httpClient := newCacheprogHTTPClient()
	httpClient.Timeout = 5 * time.Second
	t.Cleanup(httpClient.CloseIdleConnections)
	return &cacheprogRunner{
		serverURL:  serverURL,
		localDir:   localDir,
		httpClient: httpClient,
		bw:         bw,
		enc:        json.NewEncoder(bw),
	}, &buf
}

func TestHandleGetMiss(t *testing.T) {
	var requests atomic.Int32
	var connections atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.NotFound(w, r)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	runner, _ := newTestRunner(t, srv.URL)
	actionID, _ := hex.DecodeString("aa" + "00000000000000000000000000000000000000000000000000000000000000")
	resp := runner.handleGet(t.Context(), progRequest{ID: 1, ActionID: actionID})

	require.True(t, resp.Miss)
	require.Equal(t, int64(1), resp.ID)

	// An ordinary miss must not disable remote caching.
	resp = runner.handleGet(t.Context(), progRequest{ID: 2, ActionID: actionID})
	require.True(t, resp.Miss)
	require.Equal(t, int32(2), requests.Load())
	require.Equal(t, int32(1), connections.Load(), "cache misses should reuse the connection")
}

func TestCacheprogReusesConnectionsAcrossBursts(t *testing.T) {
	const workers = 8
	started := make(chan struct{}, workers)
	release := make(chan struct{}, workers)
	releaseOnce := sync.OnceFunc(func() { close(release) })
	var connections atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("X-Output-ID", "bb")
		_, _ = w.Write([]byte("cached artifact"))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	runner, _ := newTestRunner(t, srv.URL)
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	t.Cleanup(releaseOnce)

	for burst := range 2 {
		responses := make(chan progResponse, workers)
		for i := range workers {
			wg.Go(func() {
				actionID := []byte(strconv.Itoa(burst*workers + i))
				responses <- runner.handleGet(t.Context(), progRequest{ActionID: actionID})
			})
		}
		// Hold all responses until the entire burst has an active connection.
		for range workers {
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("cache requests did not start")
			}
		}
		for range workers {
			release <- struct{}{}
		}
		wg.Wait()
		close(responses)
		for resp := range responses {
			require.Empty(t, resp.Err)
			require.False(t, resp.Miss)
		}
		require.Equal(t, int32(workers), connections.Load(), "the second burst should reuse the first burst's connections")
	}
}

func TestHandleGetHit(t *testing.T) {
	blobContent := "cached build output"
	outputIDHex := "bb00000000000000000000000000000000000000000000000000000000000000"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Output-ID", outputIDHex)
		_, _ = w.Write([]byte(blobContent))
	}))
	defer srv.Close()

	runner, _ := newTestRunner(t, srv.URL)
	actionID, _ := hex.DecodeString("aa00000000000000000000000000000000000000000000000000000000000000")
	resp := runner.handleGet(t.Context(), progRequest{ID: 2, ActionID: actionID})

	require.False(t, resp.Miss)
	require.Equal(t, int64(2), resp.ID)
	require.Equal(t, int64(len(blobContent)), resp.Size)
	require.NotEmpty(t, resp.DiskPath)
	require.NotNil(t, resp.Time)

	// Verify the OutputID was decoded correctly.
	require.Equal(t, outputIDHex, hex.EncodeToString(resp.OutputID))

	// Verify blob was written to disk.
	data, err := os.ReadFile(resp.DiskPath)
	require.NoError(t, err)
	require.Equal(t, blobContent, string(data))
}

func TestHandleGetLocalCacheHit(t *testing.T) {
	// Seed the local cache so the network is never hit.
	runner, _ := newTestRunner(t, "http://should-not-be-called")

	actionHex := "cc00000000000000000000000000000000000000000000000000000000000000"
	outputIDHex := "dd00000000000000000000000000000000000000000000000000000000000000"
	localFile := filepath.Join(runner.localDir, actionHex)
	metaFile := localFile + ".meta"

	require.NoError(t, os.WriteFile(localFile, []byte("local blob"), 0o600))
	now := time.Now()
	meta := localMeta{OutputID: outputIDHex, Size: 10, Time: &now}
	metaData, _ := json.Marshal(meta)
	require.NoError(t, os.WriteFile(metaFile, metaData, 0o600))

	actionID, _ := hex.DecodeString(actionHex)
	resp := runner.handleGet(t.Context(), progRequest{ID: 3, ActionID: actionID})

	require.False(t, resp.Miss)
	require.Equal(t, int64(3), resp.ID)
	require.Equal(t, localFile, resp.DiskPath)
	require.Equal(t, int64(10), resp.Size)
}

func TestHandlePutSuccess(t *testing.T) {
	var receivedBody []byte
	var receivedOutputID string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedOutputID = r.URL.Query().Get("output_id")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	runner, _ := newTestRunner(t, srv.URL)
	actionID, _ := hex.DecodeString("ee00000000000000000000000000000000000000000000000000000000000000")
	outputID, _ := hex.DecodeString("ff00000000000000000000000000000000000000000000000000000000000000")
	body := []byte("build artifact data")

	resp := runner.handlePut(t.Context(), progRequest{
		ID:       4,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, body)

	require.Empty(t, resp.Err)
	require.Equal(t, int64(4), resp.ID)
	require.NotEmpty(t, resp.DiskPath)

	// Verify the server received the correct data.
	require.Equal(t, string(body), string(receivedBody))
	require.Equal(t, hex.EncodeToString(outputID), receivedOutputID)

	// Verify local file was written.
	data, err := os.ReadFile(resp.DiskPath)
	require.NoError(t, err)
	require.Equal(t, string(body), string(data))

	// Verify sidecar was written with Time set.
	metaFile := resp.DiskPath + ".meta"
	metaData, err := os.ReadFile(metaFile)
	require.NoError(t, err)
	var meta localMeta
	require.NoError(t, json.Unmarshal(metaData, &meta))
	require.NotNil(t, meta.Time)
	require.Equal(t, int64(len(body)), meta.Size)
}

type cacheprogRoundTripper func(*http.Request) (*http.Response, error)

func (f cacheprogRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRemoteFailureUsesLocalCache(t *testing.T) {
	for _, operation := range []string{"get", "put"} {
		for _, failure := range []string{"http_error", "connection_refused", "timeout"} {
			t.Run(operation+"/"+failure, func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if failure == "timeout" {
						_, _ = io.Copy(io.Discard, r.Body)
						<-r.Context().Done()
						return
					}
					http.Error(w, "server error", http.StatusInternalServerError)
				}))
				t.Cleanup(srv.Close)
				if failure == "connection_refused" {
					srv.Close()
				}

				runner, _ := newTestRunner(t, srv.URL)
				if failure == "timeout" {
					runner.httpClient.Timeout = 10 * time.Millisecond
				}
				var attempts atomic.Int32
				runner.httpClient.Transport = cacheprogRoundTripper(func(req *http.Request) (*http.Response, error) {
					attempts.Add(1)
					return http.DefaultTransport.RoundTrip(req)
				})
				req := progRequest{ID: 1, ActionID: []byte("action"), OutputID: []byte("output")}
				body := []byte("build artifact")
				if operation == "get" {
					resp := runner.handleGet(t.Context(), req)
					require.Empty(t, resp.Err)
					require.True(t, resp.Miss)
				} else {
					resp := runner.handlePut(t.Context(), req, body)
					require.Empty(t, resp.Err)
					data, err := os.ReadFile(resp.DiskPath)
					require.NoError(t, err)
					require.Equal(t, body, data)
				}
				require.Equal(t, int32(1), attempts.Load())

				// All later requests use local storage without another network attempt.
				put := runner.handlePut(t.Context(), req, body)
				require.Empty(t, put.Err)
				get := runner.handleGet(t.Context(), req)
				require.Empty(t, get.Err)
				require.False(t, get.Miss)
				require.Equal(t, put.DiskPath, get.DiskPath)
				require.Equal(t, req.OutputID, get.OutputID)
				require.Equal(t, int64(len(body)), get.Size)
				require.NotNil(t, get.Time)
				data, err := os.ReadFile(get.DiskPath)
				require.NoError(t, err)
				require.Equal(t, body, data)
				miss := runner.handleGet(t.Context(), progRequest{ID: 2, ActionID: []byte("missing")})
				require.True(t, miss.Miss)
				require.Empty(t, miss.Err)
				require.Equal(t, int32(1), attempts.Load())
			})
		}
	}
}

func TestHandleGetInterruptedDownloadDisablesRemote(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Output-ID", "bb00000000000000000000000000000000000000000000000000000000000000")
				w.Header().Set("Content-Length", "100")
				w.WriteHeader(status)
				_, _ = w.Write([]byte("truncated"))
			}))
			defer srv.Close()
			runner, _ := newTestRunner(t, srv.URL)
			resp := runner.handleGet(t.Context(), progRequest{ID: 1, ActionID: []byte("action")})
			require.True(t, resp.Miss)
			require.Empty(t, resp.Err)
			require.True(t, runner.remoteDisabled.Load())
			files, err := os.ReadDir(runner.localDir)
			require.NoError(t, err)
			require.Empty(t, files)
		})
	}
}

func TestHandlePutLocalFailure(t *testing.T) {
	for _, failure := range []string{"create", "rename"} {
		t.Run(failure, func(t *testing.T) {
			runner, _ := newTestRunner(t, "http://unused")
			req := progRequest{ID: 1, ActionID: []byte("action"), OutputID: []byte("output")}
			if failure == "create" {
				runner.localDir = filepath.Join(runner.localDir, "missing")
			} else {
				// A directory at the artifact path prevents the final rename.
				require.NoError(t, os.Mkdir(filepath.Join(runner.localDir, hex.EncodeToString(req.ActionID)), 0o700))
			}
			var attempts atomic.Int32
			runner.httpClient.Transport = cacheprogRoundTripper(func(req *http.Request) (*http.Response, error) {
				attempts.Add(1)
				return nil, errors.New("unexpected remote request")
			})
			resp := runner.handlePut(t.Context(), req, []byte("data"))
			require.NotEmpty(t, resp.Err)
			require.Empty(t, resp.DiskPath)
			require.Zero(t, attempts.Load())
		})
	}
}

func TestConcurrentRemoteFailuresWarnOnce(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	runner, _ := newTestRunner(t, "http://unused")
	const workers = 8
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	t.Cleanup(releaseOnce)
	runner.httpClient.Transport = cacheprogRoundTripper(func(req *http.Request) (*http.Response, error) {
		started <- struct{}{}
		<-release
		return nil, errors.New("remote unavailable")
	})
	responses := make(chan progResponse, workers)
	for i := range workers {
		wg.Go(func() {
			actionID := []byte(strconv.Itoa(i))
			responses <- runner.handlePut(t.Context(), progRequest{
				ID: int64(i + 1), ActionID: actionID, OutputID: actionID,
			}, []byte("data"))
		})
	}
	// Ensure all uploads are in flight before they fail together.
	for range workers {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("uploads did not start")
		}
	}
	releaseOnce()
	wg.Wait()
	close(responses)
	for resp := range responses {
		require.Empty(t, resp.Err)
		data, err := os.ReadFile(resp.DiskPath)
		require.NoError(t, err)
		require.Equal(t, "data", string(data))
	}
	require.Equal(t, 1, strings.Count(logs.String(), "remote cache unavailable"))
}

func TestRunProtocol(t *testing.T) {
	// Test the full stdin/stdout protocol loop with just a close command
	// to verify the capability handshake and graceful shutdown.
	localDir := t.TempDir()

	// Build the input stream: just a close command.
	var input bytes.Buffer
	enc := json.NewEncoder(&input)
	_ = enc.Encode(progRequest{ID: 1, Command: "close"})

	// Redirect stdin/stdout for the runner.
	origStdin := os.Stdin
	origStdout := os.Stdout
	defer func() {
		os.Stdin = origStdin
		os.Stdout = origStdout
	}()

	stdinR, stdinW, _ := os.Pipe()
	stdoutR, stdoutW, _ := os.Pipe()

	os.Stdin = stdinR
	os.Stdout = stdoutW

	// Write input in a goroutine.
	go func() {
		_, _ = stdinW.Write(input.Bytes())
		_ = stdinW.Close()
	}()

	bw := bufio.NewWriter(stdoutW)
	runner := &cacheprogRunner{
		serverURL:  "http://localhost:0",
		localDir:   localDir,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		bw:         bw,
		enc:        json.NewEncoder(bw),
	}

	err := runner.run()
	_ = stdoutW.Close()
	require.NoError(t, err)

	// Read and verify responses.
	var responses []progResponse
	dec := json.NewDecoder(stdoutR)
	for {
		var resp progResponse
		if err := dec.Decode(&resp); err != nil {
			break
		}
		responses = append(responses, resp)
	}

	require.Len(t, responses, 2) // capability + close
	require.Equal(t, int64(0), responses[0].ID)
	require.Contains(t, responses[0].KnownCommands, "get")
	require.Contains(t, responses[0].KnownCommands, "put")
	require.Contains(t, responses[0].KnownCommands, "close")
	require.Equal(t, int64(1), responses[1].ID)
}

func TestRunProtocolEOF(t *testing.T) {
	// Test that the runner exits cleanly on EOF (stdin closed without close command).
	localDir := t.TempDir()

	origStdin := os.Stdin
	origStdout := os.Stdout
	defer func() {
		os.Stdin = origStdin
		os.Stdout = origStdout
	}()

	stdinR, stdinW, _ := os.Pipe()
	_, stdoutW, _ := os.Pipe()

	os.Stdin = stdinR
	os.Stdout = stdoutW

	// Close stdin immediately after runner starts reading.
	go func() {
		_ = stdinW.Close()
	}()

	bw := bufio.NewWriter(stdoutW)
	runner := &cacheprogRunner{
		serverURL:  "http://localhost:0",
		localDir:   localDir,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		bw:         bw,
		enc:        json.NewEncoder(bw),
	}

	err := runner.run()
	_ = stdoutW.Close()
	require.NoError(t, err)
}

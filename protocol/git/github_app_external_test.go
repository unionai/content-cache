package git_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildkite/content-cache/protocol/git"
	"github.com/stretchr/testify/require"
)

func TestGitHubAppAuthPublicOptionsSupportExternalTests(t *testing.T) {
	_, privateKeyPEM := testExternalGitHubAppPrivateKeyPEM(t)
	now := time.Date(2026, 5, 21, 12, 0, 0, 0, time.UTC)

	var sawCustomClient atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Client") == "custom" {
			sawCustomClient.Store(true)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "external-token",
			"expires_at": now.Add(time.Hour),
		})
	}))
	t.Cleanup(api.Close)

	client := &http.Client{Transport: headerTransport{
		base:   http.DefaultTransport,
		header: "X-Test-Client",
		value:  "custom",
	}}

	auth, err := git.NewGitHubAppAuth(git.GitHubAppAuthConfig{
		AppID:          "12345",
		InstallationID: "67890",
		PrivateKey:     privateKeyPEM,
		TokenScope:     git.GitHubAppTokenScopeRequestedRepo,
	},
		git.WithGitHubAppAPIURL(api.URL),
		git.WithGitHubAppClock(func() time.Time { return now }),
		git.WithGitHubAppHTTPClient(client),
	)
	require.NoError(t, err)

	username, password, err := auth.BasicAuth(context.Background(), git.RepoRef{
		Host:     "github.com",
		RepoPath: "buildkite/content-cache",
	})
	require.NoError(t, err)
	require.Equal(t, "x-access-token", username)
	require.Equal(t, "external-token", password)
	require.True(t, sawCustomClient.Load())
}

type headerTransport struct {
	base   http.RoundTripper
	header string
	value  string
}

func (t headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set(t.header, t.value)
	return t.base.RoundTrip(req)
}

func testExternalGitHubAppPrivateKeyPEM(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}
	return key, string(pem.EncodeToMemory(block))
}

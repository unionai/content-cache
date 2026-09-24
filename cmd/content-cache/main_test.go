package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCLIParserCacheprogIgnoresServerEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
	}{
		{"LOG_LEVEL", "5"},
		{"LOG_FORMAT", "custom"},
		{"CACHE_MAX_SIZE", "not-an-integer"},
		{"METRICS_PROMETHEUS", "not-a-boolean"},
		{"GC_INTERVAL", "not-a-duration"},
		{"TLS_CERT_FILE", "/nonexistent/cacheprog-test-cert.pem"},
		{"MAVEN_UPSTREAM", "invalid-url"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			t.Setenv("CONTENT_CACHE_SERVER", "http://cache.example:8080")
			localDir := t.TempDir()
			t.Setenv("CONTENT_CACHE_LOCAL_DIR", localDir)
			var cli CLI
			args := []string{"cacheprog"}
			parser, err := newCLIParser(&cli, args, io.Discard, io.Discard)
			require.NoError(t, err)
			ctx, err := parser.Parse(args)
			require.NoError(t, err)
			require.Equal(t, "cacheprog", ctx.Command())
			require.Equal(t, "http://cache.example:8080", cli.Cacheprog.Server)
			require.Equal(t, localDir, cli.Cacheprog.LocalDir)
		})
	}
}

func TestCLIParserServeLogLevel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		level   string
		wantErr bool
	}{
		{"default server", nil, "debug", false},
		{"explicit server", []string{"serve"}, "warn", false},
		{"invalid default server", nil, "5", true},
		{"invalid explicit server", []string{"serve"}, "5", true},
		{"flag overrides environment", []string{"serve", "--log-level=error"}, "5", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", tc.level)
			var cli CLI
			parser, err := newCLIParser(&cli, tc.args, io.Discard, io.Discard)
			require.NoError(t, err)
			ctx, err := parser.Parse(tc.args)
			if tc.wantErr {
				require.ErrorContains(t, err, "--log-level must be one of")
				return
			}
			require.NoError(t, err)
			require.Equal(t, "serve", ctx.Command())
			if tc.name == "flag overrides environment" {
				require.Equal(t, "error", cli.Serve.LogLevel)
			} else {
				require.Equal(t, tc.level, cli.Serve.LogLevel)
			}
		})
	}
}

func TestCLIParserCacheprogDiagnosticsUseStderr(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		exit int
	}{
		{"help", []string{"cacheprog", "--help"}, 0},
		{"missing server", []string{"cacheprog"}, 80},
		{"invalid flag", []string{"cacheprog", "--unknown"}, 80},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", "5")
			t.Setenv("CONTENT_CACHE_SERVER", "")
			require.NoError(t, os.Unsetenv("CONTENT_CACHE_SERVER"))
			var cli CLI
			var stdout, stderr bytes.Buffer
			parser, err := newCLIParser(&cli, tc.args, &stdout, &stderr)
			require.NoError(t, err)
			parser.Exit = func(code int) {
				require.Equal(t, tc.exit, code)
				panic("CLI exit")
			}
			require.PanicsWithValue(t, "CLI exit", func() {
				_, err := parser.Parse(tc.args)
				parser.FatalIfErrorf(err)
			})
			require.Empty(t, stdout.String())
			require.Contains(t, stderr.String(), "Usage: content-cache cacheprog")
			if tc.name == "missing server" {
				require.Contains(t, stderr.String(), "--server")
			}
		})
	}
}

func TestServeCmdValidateMavenUpstream(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		urls    []string
		wantErr string
	}{
		{name: "unset falls back to default", urls: nil},
		{name: "single valid", urls: []string{"https://repo.maven.apache.org/maven2"}},
		{name: "multiple valid", urls: []string{"https://a.example/m", "http://b.example"}},
		{name: "trailing slash ok", urls: []string{"https://a.example/m/"}},
		{name: "whitespace trimmed", urls: []string{"  https://a.example  "}},

		{name: "empty string", urls: []string{""}, wantErr: "URL is empty"},
		{name: "whitespace only", urls: []string{"   "}, wantErr: "URL is empty"},
		{name: "second empty", urls: []string{"https://a.example", ""}, wantErr: "--maven-upstream[1]"},
		{name: "missing scheme", urls: []string{"repo.maven.apache.org"}, wantErr: "scheme must be"},
		{name: "wrong scheme", urls: []string{"ftp://a.example"}, wantErr: "scheme must be"},
		{name: "missing host", urls: []string{"https://"}, wantErr: "missing host"},
		{name: "duplicate", urls: []string{"https://a.example/m", "https://a.example/m/"}, wantErr: "duplicated"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := &ServeCmd{MavenUpstream: tc.urls}
			err := cmd.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

package server

import (
	"testing"

	"github.com/buildkite/content-cache/credentials"
	"github.com/stretchr/testify/require"
)

func TestNewRejectsGitHubAppRoutesWithoutTrustedSingleTenant(t *testing.T) {
	_, err := New(Config{
		Credentials: credentialsWithGitHubAppRoute(),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "trusted single-tenant")
}

func TestValidateGitUpstreamAuth_AllowsTrustedSingleTenantGitHubApp(t *testing.T) {
	err := validateGitUpstreamAuth(Config{
		Credentials:                        credentialsWithGitHubAppRoute(),
		GitUpstreamAuthTrustedSingleTenant: true,
	})
	require.NoError(t, err)
}

func TestValidateGitUpstreamAuth_RejectsInvalidGitHubAppRoutes(t *testing.T) {
	tests := []struct {
		name    string
		route   credentials.GitRoute
		wantErr string
	}{
		{
			name: "combined with static auth",
			route: credentials.GitRoute{
				Match:    credentials.GitRouteMatch{RepoPrefix: "github.com/buildkite/"},
				Username: "x-access-token",
				Password: "pat",
				GitHubApp: &credentials.GitHubAppConfig{
					AppID:          "12345",
					InstallationID: "67890",
					PrivateKey:     "private key",
					TokenScope:     "requested_repo",
				},
			},
			wantErr: "cannot be combined",
		},
		{
			name: "catch-all",
			route: credentials.GitRoute{
				Match: credentials.GitRouteMatch{Any: true},
				GitHubApp: &credentials.GitHubAppConfig{
					AppID:          "12345",
					InstallationID: "67890",
					PrivateKey:     "private key",
					TokenScope:     "requested_repo",
				},
			},
			wantErr: "catch-all",
		},
		{
			name: "non github prefix",
			route: credentials.GitRoute{
				Match: credentials.GitRouteMatch{RepoPrefix: "gitlab.com/buildkite/"},
				GitHubApp: &credentials.GitHubAppConfig{
					AppID:          "12345",
					InstallationID: "67890",
					PrivateKey:     "private key",
					TokenScope:     "requested_repo",
				},
			},
			wantErr: "github.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateGitUpstreamAuth(Config{
				Credentials: &credentials.Credentials{
					Git: &credentials.GitAuthConfig{
						Routes: []credentials.GitRoute{
							tc.route,
							{Match: credentials.GitRouteMatch{Any: true}},
						},
					},
				},
				GitUpstreamAuthTrustedSingleTenant: true,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func credentialsWithGitHubAppRoute() *credentials.Credentials {
	return &credentials.Credentials{
		Git: &credentials.GitAuthConfig{
			Routes: []credentials.GitRoute{
				{
					Match: credentials.GitRouteMatch{RepoPrefix: "github.com/buildkite/"},
					GitHubApp: &credentials.GitHubAppConfig{
						AppID:          "12345",
						InstallationID: "67890",
						PrivateKey:     "private key",
						TokenScope:     "requested_repo",
					},
				},
				{Match: credentials.GitRouteMatch{Any: true}},
			},
		},
	}
}
